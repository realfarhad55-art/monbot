#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""monbot.py - WizardScan X-MILESTONE monitor bot (SECOND bot, standalone).

This bot is SEPARATE from the main wizu bot. It does ONE job: watch the
main bot's tracked calls for X milestones (and dip-profit milestones) and
queue them for the main bot to post. It NEVER posts to Telegram itself.

DESIGN (same safe pattern as dropbot, Nov 24 2026):
- The main bot monitors 3,339 tracked calls and runs at ~113% CPU. Its own
  monitoring jobs are switched OFF (MONBOT_MODE=1, see
  apply_monbot_mode_nov24.py). This bot takes over detection only.
- This bot reads the main bot's data READ-ONLY:
  /opt/bot/data/tracked_calls.json, sent_milestones.json, bot_config.json,
  channels.json (+ priority_channels.json if present). It NEVER writes them.
- When a milestone is hit, this bot writes it to its OWN file only:
  /opt/monbot/data/monbot_milestones.json (atomic write: temp + os.replace).
- The main bot's monbot_import_job reads that queue every 1s and POSTS the
  X alert itself (via rt_enqueue_milestone) and seals the milestone in
  sent_milestones.json. The main bot owns ALL posting and ALL record writes.
- This bot needs NO bot token and NO userbot session. Do NOT add Telegram
  credentials here. Sharing a userbot session between two bots disconnects
  one of them (that is what hit dropbot on Nov 24 with @Wizard_Scan).

DATA SAFETY (hard rule):
- No open(..., "w") and no _save_tracked / _save_milestones /
  _save_milestone_posts calls targeting main bot files anywhere in this
  file. The only files this bot writes are under MONBOT_DATA_DIR:
  monbot_milestones.json (queue), monbot_peaks.json (peak/spike state),
  monbot_dip.json (dip-low state).
- Call dicts are this bot's PRIVATE deep copies. Mutating them cannot leak
  anywhere, because the main bot's files are only ever read, never written.

DETECTION (same logic as the main bot's check_one, x_alert.py):
- Bulk DexScreener cache first, bounded per-token fallback, alt-source
  probe only when DexScreener is stuck near 1.0x, rug gate, spike-glitch
  guard, peak tracking, entry-confirmation gate (fail-safe 120s / 420s RH).
- GUARD RULE (from spec, GUARD_NOV23 behaviour): a milestone is queued
  ONLY if the LIVE ratio from THIS tick is at or above the level. A peak
  above the level with live below it is NOT queued (the token may recover
  and fire later; if it never recovers it never fires - that is the point).
- What this bot does NOT do (main bot keeps these, or v1 drops them):
  entry fill / reconcile, _confirm_entry_mc task, early-rebase guard,
  _remove_impossible_milestones writes, failed-call point deduction,
  hot-streak registration, series sampling writes, watchdog dropped-alert
  requeue. See monbot-deploy.md (known gaps).

ENV: .env file next to this script (see monbot.env.example). No token or
session vars exist here on purpose.
"""

# --- imports ---
import os
import sys
import re
import io
import json
import time
import copy
import signal
import logging
import requests
import asyncio
import functools
import threading
import contextvars as _contextvars
import time as _time
import threading as _threading
import asyncio as _asyncio
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

# --- .env loader (same tiny loader as dropbot.py, no external dependency) ---
def _load_dotenv(path):
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

_ENV_PATH = os.environ.get("MONBOT_ENV", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".env"))
_load_dotenv(_ENV_PATH)

# --- logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S")
logger = logging.getLogger("monbot")

# --- env / paths ---
MONBOT_DATA_DIR = os.environ.get("MONBOT_DATA_DIR", "/opt/monbot/data")
MONBOT_MAIN_DATA_DIR = os.environ.get("MONBOT_MAIN_DATA_DIR", "/opt/bot/data")
MONBOT_TICK_SECONDS = float(os.environ.get("MONBOT_TICK_SECONDS", "2") or 2)
MONBOT_PRIO_SECONDS = float(os.environ.get("MONBOT_PRIO_SECONDS", "1") or 1)
MONBOT_QUEUE_FILE = os.environ.get(
    "MONBOT_QUEUE_FILE", os.path.join(MONBOT_DATA_DIR, "monbot_milestones.json"))
MONBOT_PEAKS_FILE = os.environ.get(
    "MONBOT_PEAKS_FILE", os.path.join(MONBOT_DATA_DIR, "monbot_peaks.json"))
MONBOT_DIP_FILE = os.environ.get(
    "MONBOT_DIP_FILE", os.path.join(MONBOT_DATA_DIR, "monbot_dip.json"))
os.makedirs(MONBOT_DATA_DIR, exist_ok=True)

# Main bot data files (READ-ONLY - this bot never writes them).
MAIN_TRACKED_FILE = os.path.join(MONBOT_MAIN_DATA_DIR, "tracked_calls.json")
MAIN_MILESTONES_FILE = os.path.join(MONBOT_MAIN_DATA_DIR, "sent_milestones.json")
MAIN_CONFIG_FILE = os.path.join(MONBOT_MAIN_DATA_DIR, "bot_config.json")
MAIN_CHANNELS_FILE = os.path.join(MONBOT_MAIN_DATA_DIR, "channels.json")
MAIN_PRIO_FILE = os.path.join(MONBOT_MAIN_DATA_DIR, "priority_channels.json")
# Fallback copies in monbot's own data dir (deploy step 3). Only used if
# the live main-bot file is missing; the live file is always preferred.
FALLBACK_CONFIG_FILE = os.path.join(MONBOT_DATA_DIR, "bot_config.json")
FALLBACK_CHANNELS_FILE = os.path.join(MONBOT_DATA_DIR, "channels.json")
FALLBACK_PRIO_FILE = os.path.join(MONBOT_DATA_DIR, "priority_channels.json")

# --- small file helpers (only ever used on MONBOT's own files, or reads) ---
def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _read_json_file(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if data is not None else default
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.warning(f"monbot: read failed {path}: {e}")
        return default


# --- config / channels shims (read from MAIN data dir, cached by mtime) ---
# The extracted live code below calls load_config(), load_channels() and
# priority_channels_lower(). These shims satisfy those calls with read-only
# loads of the main bot's files. Nothing here ever writes.
_config_cache = {"mtime": 0.0, "data": {}}
_channels_cache = {"mtime": 0.0, "data": []}
_prio_cache = {"mtime": 0.0, "data": set()}


def load_config():
    for path in (MAIN_CONFIG_FILE, FALLBACK_CONFIG_FILE):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime != _config_cache["mtime"] or _config_cache.get("path") != path:
            _config_cache["data"] = _read_json_file(path, {}) or {}
            _config_cache["mtime"] = mtime
            _config_cache["path"] = path
        return _config_cache["data"]
    return dict(_config_cache["data"])


def load_channels():
    for path in (MAIN_CHANNELS_FILE, FALLBACK_CHANNELS_FILE):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime != _channels_cache["mtime"] or _channels_cache.get("path") != path:
            data = _read_json_file(path, []) or []
            if isinstance(data, dict):
                data = list(data.keys())
            _channels_cache["data"] = list(data)
            _channels_cache["mtime"] = mtime
            _channels_cache["path"] = path
        return list(_channels_cache["data"])
    return list(_channels_cache["data"])


def priority_channels_lower():
    for path in (MAIN_PRIO_FILE, FALLBACK_PRIO_FILE):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime != _prio_cache["mtime"] or _prio_cache.get("path") != path:
            data = _read_json_file(path, []) or []
            if isinstance(data, dict):
                data = list(data.keys())
            _prio_cache["data"] = {str(c).lower() for c in data}
            _prio_cache["mtime"] = mtime
            _prio_cache["path"] = path
        return set(_prio_cache["data"])
    return set(_prio_cache["data"])


# === EXTRACTED LIVE CODE (verbatim, Nov 23 base copies) =====================
# Functions and constants below are copied byte-for-byte from
# core-fadf8802.py and x_alert-3aa26821.py by AST extraction (see the build
# notes in monbot-deploy.md), with dependency closure verified by symtable.
# Only adaptation: em-dashes in comments were replaced with plain hyphens.
# Names this file provides itself (load_config, load_channels,
# priority_channels_lower, logger) are NOT in this block.
# Extracted verbatim from Nov 23 base copies - see monbot.py header.


X_MILESTONES = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 18, 20, 22, 25, 28,
                30, 32, 34, 35, 38, 40, 45, 50,
                55, 60, 65, 75, 100, 150, 200, 250, 300,  # SUPPRESS_LEVELS_NOV14: 70 removed - no 70x alerts
                500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000,
                6000, 7000, 8000, 9000, 10000,
                11000, 12000, 13000, 14000, 15000, 20000, 25000, 30000,
                40000, 50000, 60000, 70000, 80000, 90000, 100000]


CHAMPION_MILESTONES = [2, 5, 8, 10, 15, 20, 22, 25, 28, 30, 35, 40, 45, 50, 55, 60, 65, 75, 80, 90, 100, 150, 200, 250, 300, 500, 600, 700, 800, 900, 1000]  # CHAMPION_VIDEO_FINAL_NOV10


MAX_MILESTONE = 1_000


SUPPORTED_CHAINS = {"ethereum": "ETH", "bsc": "BNB", "base": "BASE", "solana": "SOL",
                    "robinhood": "RH", "ton": "TON", "arc": "ARC"}   # Robinhood L2 (EVM, OP-Stack); ARC = Circle's new L1 (EVM, launched 2026-09-16)


CHAIN_TO_DEXPATH = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base",
                    "RH": "robinhood", "TON": "ton", "ARC": "arc"}


def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except Exception: return default


def get_milestones():
    cfg = load_config()
    stored = cfg.get("custom_milestones")
    if stored:
        try: base = sorted(set(int(x) for x in stored))
        except Exception: base = list(X_MILESTONES)
    else:
        base = list(X_MILESTONES)
    # Also include X values for which milestone_media is set, so those X levels are always tracked
    try:
        media_keys = cfg.get("milestone_media", {}).keys()
        extra = [int(k) for k in media_keys if k.lstrip('-').isdigit() and int(k) <= MAX_MILESTONE]
        if extra:
            base = sorted(set(base) | set(extra))
    except Exception:
        pass
    return base


def fmt_mc(value):
    """Format market cap without a currency sign, e.g. 12.5K MC or 3.45M MC."""
    if not value: return "N/A"
    if value >= 1_000_000_000: return f"{value/1_000_000_000:.2f}B MC"
    elif value >= 1_000_000:   return f"{value/1_000_000:.2f}M MC"
    elif value >= 1_000:       return f"{value/1_000:.1f}K MC"
    return f"{value:.0f} MC"


RUG_MIN_LIQUIDITY  = float(os.environ.get("RUG_MIN_LIQUIDITY",  "150"))    # USD


RUG_LIQ_DROP_RATIO = float(os.environ.get("RUG_LIQ_DROP_RATIO", "0.15"))   # vs peak liq


MAX_MC_LIQ_RATIO   = float(os.environ.get("MAX_MC_LIQ_RATIO",   "5000"))   # mc / liquidity


MAX_PLAUSIBLE_MC   = float(os.environ.get("MAX_PLAUSIBLE_MC",   "2000000000"))  # $2B


RUG_MIN_VOL_H24    = float(os.environ.get("RUG_MIN_VOL_H24",    "50"))     # USD


MC_PRICE_PUMP_GUARD = os.environ.get("MC_PRICE_PUMP_GUARD", "1").strip().lower() not in ("0", "false", "off")


LIVE_MIN_VOL_H24   = float(os.environ.get("LIVE_MIN_VOL_H24",   "500"))    # USD


def _rug_status(call, dex):
    """Return (ok: bool, reason: str).

    ok=False means: do NOT post any alert for this call on this tick.
    Truly dead tokens are permanently marked rugged+frozen so they never
    generate another alert.

    Every HARD-stop reason is logged with the raw numbers that caused it -
    this is what shows up in Railway logs when a channel's live /record
    check displays a real X but the milestone never posts to the channel,
    so the owner can see exactly why a specific tick was blocked.
    """
    try:
        liq = float(dex.get("liquidity_usd", 0) or 0)
        mc  = float(dex.get("mcap", 0) or 0)
        vol = float(dex.get("volume_h24", 0) or 0)
    except (TypeError, ValueError):
        logger.warning(f"🚫 rug-gate bad_data {call.get('symbol','?')} @{call.get('channel','?')} "
                       f"raw_dex={dex!r}")
        return (False, "bad_data")

    if call.get("rugged"):
        logger.info(f"⏸️ rug-gate already_rugged {call.get('symbol','?')} @{call.get('channel','?')} "
                    f"(frozen={call.get('frozen')}, reason={call.get('rug_reason')}) - "
                    f"use /unfreezecall {call.get('ca','')} to resume if this was a false positive")
        return (False, "already_rugged")

    # remember the healthiest liquidity we ever saw for this call
    try:
        peak_liq = float(call.get("peak_liq", 0) or 0)
    except (TypeError, ValueError):
        peak_liq = 0.0
    if liq > peak_liq:
        call["peak_liq"] = liq
        peak_liq = liq

    if mc <= 0:
        logger.warning(f"🚫 rug-gate no_mc {call.get('symbol','?')} @{call.get('channel','?')} "
                       f"liq=${liq:.0f} vol24h=${vol:.0f} - will retry next tick")
        return (False, "no_mc")
    if mc > MAX_PLAUSIBLE_MC:
        logger.warning(f"🚫 rug-gate absurd_mc {call.get('symbol','?')} @{call.get('channel','?')} "
                       f"mc=${mc:.0f} (cap ${MAX_PLAUSIBLE_MC:.0f}) liq=${liq:.0f} vol24h=${vol:.0f} "
                       f"- will retry next tick")
        return (False, "absurd_mc")

    # Alternative providers intentionally return price/MC but do not expose
    # the pair's liquidity and 24h volume in the same response shape as
    # DexScreener.  Treating their zero-filled fields as "liquidity pulled"
    # froze perfectly live calls before they could reach the milestone engine.
    # MC is still checked above; only the unavailable pool-health checks are
    # skipped for a clearly identified external-source quote.
    if dex.get("_source") and dex.get("_source") != "dexscreener":
        return (True, "external_source")

    # Liquidity pulled → rug. Permanent stop.
    if liq <= 0 or (peak_liq > 0 and liq < peak_liq * RUG_LIQ_DROP_RATIO
                    and liq < RUG_MIN_LIQUIDITY * 4):
        # LIVENESS GUARD: a real rug kills trading immediately, so liq=$0
        # (or a liq collapse) alongside strong real 24h volume can't be an
        # actual rug - it's a DexScreener data glitch/pool-swap blip on the
        # liquidity field specifically. Permanently freezing those calls was
        # exactly the bug that made real 9x/10x calls stop alerting for
        # hours: they hit one bad liq=0 tick, got frozen forever, and only
        # ever resumed if the owner manually ran /unfreezecall. Treat this
        # as a transient miss instead - it auto-retries next tick and
        # resumes on its own once the liquidity field comes back, with no
        # manual action needed and no real rug ever slipping through
        # (vol < LIVE_MIN_VOL_H24 still falls through to the permanent path
        # below, unchanged).
        # OWNER REQUIREMENT (2026-10-09): liquidity exactly $0 = RUG, even if
        # vol24h is high (24h volume is history and a rugged token still shows
        # it). Exact-zero liquidity skips the liveness guard and goes through
        # the RUG_CONFIRM_STRIKES path below, which freezes the call; the
        # cleanup job then drops it from tracking. Partial liq collapse
        # (liq>0 but tiny vs peak) keeps the old volume guard.
        # Refinement after live logs (2026-10-09): peak_liq == 0 means this call
        # NEVER had any liquidity reading (e.g. pump.fun bonding-curve tokens
        # whose pool DexScreener does not report yet) - that is "unknown",
        # not "pulled", so it keeps the old volume liveness guard. Only a call
        # that HAD liquidity (peak_liq > 0) and is now exactly $0 is a rug.
        if vol >= LIVE_MIN_VOL_H24 and (liq > 0 or peak_liq <= 0):
            call["_rug_strikes"] = 0
            logger.warning(f"⏳ rug-gate liq_glitch {call.get('symbol','?')} @{call.get('channel','?')} "
                           f"liq=${liq:.0f} peak=${peak_liq:.0f} mc=${mc:.0f} vol24h=${vol:.0f} "
                           f"- vol24h proves it's alive, NOT rugging (data glitch) - will retry next tick")
            return (False, "liq_glitch_retry")
        # FIX (freeze bug, second layer): the volume guard above only saves a
        # call when THAT SAME tick also shows healthy volume. A malformed/
        # partial DexScreener payload (common right when a 429 storm is
        # recovering) can report BOTH liq=0 and vol=0 in the same bad tick -
        # that combination used to hit the permanent-rug branch immediately
        # below on a single reading. Require RUG_CONFIRM_STRIKES consecutive
        # bad ticks (default 3) before making it permanent; any tick with
        # healthy liquidity resets the counter to zero.
        RUG_CONFIRM_STRIKES = max(1, int(os.environ.get("RUG_CONFIRM_STRIKES", "3") or 3))
        strikes = int(call.get("_rug_strikes", 0) or 0) + 1
        call["_rug_strikes"] = strikes
        if strikes < RUG_CONFIRM_STRIKES:
            logger.info(f"🟡 rug-gate strike {strikes}/{RUG_CONFIRM_STRIKES} "
                        f"{call.get('symbol','?')} @{call.get('channel','?')} "
                        f"liq=${liq:.0f} peak=${peak_liq:.0f} vol24h=${vol:.0f} - "
                        f"not yet permanent, retrying")
            return (False, "liquidity_pulled_pending")
        call["rugged"] = True
        call["frozen"] = True
        call["rug_reason"] = "liquidity_pulled"
        call["rugged_at"] = datetime.utcnow().isoformat()
        logger.warning(f"🛑 RUG detected {call.get('symbol','?')} @{call.get('channel','?')} "
                       f"liq=${liq:.0f} peak=${peak_liq:.0f} mc=${mc:.0f} vol24h=${vol:.0f} "
                       f"(confirmed over {strikes} consecutive ticks) - "
                       f"alerts stopped PERMANENTLY. If this token is actually still live "
                       f"(DexScreener glitch/pool switch), fix with: "
                       f"/unfreezecall {call.get('ca','')}  then  "
                       f"/recheckx @{call.get('channel','?')} {call.get('ca','')}")
        return (False, "liquidity_pulled")
    elif call.get("_rug_strikes"):
        call["_rug_strikes"] = 0

    # ── LIVENESS OVERRIDE ───────────────────────────────────────────────
    # Real 24h volume = real buyers and sellers right now. DexScreener often
    # reports only one thin pool for a multi-pool token, which made the
    # depth heuristics below silently swallow perfectly live calls.
    if vol >= LIVE_MIN_VOL_H24 and liq > 0:
        return (True, "ok")

    if liq < RUG_MIN_LIQUIDITY and vol < RUG_MIN_VOL_H24:
        return (False, "low_liquidity")

    # Phantom marketcap: DexScreener FDV/marketCap of a dead token vs. its real
    # liquidity - only meaningful when the chart is also dead.
    if mc > liq * MAX_MC_LIQ_RATIO and vol < LIVE_MIN_VOL_H24:
        logger.warning(f"⚠️ Ignoring phantom MC {fmt_mc(mc)} vs liq ${liq:.0f} "
                       f"for {call.get('symbol','?')} @{call.get('channel','?')}")
        return (False, "mc_liquidity_mismatch")

    # Dead chart (no trading in 24h) - nothing real to celebrate.
    if vol < RUG_MIN_VOL_H24:
        return (False, "no_volume")

    return (True, "ok")


_dex_cache: dict = {}


_DEX_CACHE_TTL = float(os.environ.get("DEX_CACHE_TTL_SECONDS", "2.5") or 2.5)


def _pick_market_cap(pairs: list) -> tuple[float, bool]:
    """Return (market_cap, is_fdv) - the most trustworthy market cap from a
    DexScreener pairs list, and whether it had to fall back to FDV.

    DexScreener reports marketCap/fdv per pair and a single stale pair can be
    badly wrong. We therefore:
      1. take marketCap from the deepest-liquidity pair,
      2. cross-check it against the median marketCap of all pairs of the same
         token - if it is more than 3x off, trust the median instead,
      3. never substitute FDV for real market cap AS IF it were real.

    FIX (RH-chain "called at 19K, bot shows 119K instantly" incident,
    2026-09-24): point 3 used to mean "return 0.0" whenever no pair had a
    real marketCap field - which DexScreener commonly never populates for
    Robinhood-chain (tokenized-stock) pairs, only `fdv`. A silent 0.0 here
    means _reconcile_entry_mc sees "nothing to compare the caller's post-hint
    against" and blindly LOCKS IN that hint as fully "confirmed", with the
    entire cross-check/correction machinery (_confirm_entry_mc, the pre-post
    settle check, the _mc_is_fdv ratio guard) never engaging at all - because
    all of that machinery only ever triggers off an explicit _mc_is_fdv flag,
    and this path never set one. A caller posting (or a bot mis-parsing) a
    lowballed "called at 2.7K" MC then rode straight through to a real 65K
    chart as a fake "24X", with zero of the anti-cheat checks built for
    exactly this scenario ever firing.
    Now: FDV is still never trusted AS real MC (nothing changes for the
    normal case), but it IS returned as a flagged, provisional number instead
    of a bare 0 - so callers correctly route it through the same
    live_fdv/post_unconfirmed provisional path already built for alt sources
    (Birdeye/GeckoTerminal), which locks it in only after an independent
    cross-check confirms it.
    """
    mcs = []
    for p in pairs:
        try:
            v = float(p.get("marketCap") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v > 0:
            mcs.append(v)
    if mcs:
        primary = mcs[0]
        if len(mcs) >= 3:
            ordered = sorted(mcs)
            median = ordered[len(ordered) // 2]
            if median > 0 and (primary > median * 3 or primary * 3 < median):
                return (median, False)
        return (primary, False)
    # No pair has a real circulating marketCap - fall back to the
    # deepest-liquidity pair's FDV, flagged, rather than a bare 0.0.
    for p in sorted(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0,
                     reverse=True):
        try:
            fdv = float(p.get("fdv") or 0)
        except (TypeError, ValueError):
            fdv = 0.0
        if fdv > 0:
            return (fdv, True)
    return (0.0, False)


def _parse_dex_pairs(pairs: list, min_liquidity: int, token_address: str = "", prefer_chain: str = None) -> dict | None:
    """Parse a DexScreener pairs list into our internal dict. Returns None if unusable."""
    sup = [p for p in pairs if p.get("chainId","").lower() in SUPPORTED_CHAINS
           and (p.get("liquidity",{}).get("usd") or 0) >= min_liquidity]
    # priceUsd always describes baseToken.  A searched contract can also occur
    # as quoteToken; treating that pair as the call token silently tracks the
    # other asset (often SOL/WETH) and leaves the call stuck around 1x.
    if token_address:
        wanted = token_address.lower()
        sup = [p for p in sup if ((p.get("baseToken") or {}).get("address") or "").lower() == wanted]
    # WIZCHAIN_NOV23: when the caller knows the chain (e.g. the KOL's pair
    # link was on BSC), only pairs on that chain count. A multi-chain token
    # must not resolve to a different chain's pair (newest-pair guard /
    # ETH-first lookup). Safe fallback: if no pair matches, sup is unchanged.
    if prefer_chain:
        _pc = str(prefer_chain).lower()
        _on = [p for p in sup if (p.get("chainId") or "").lower() == _pc]
        if _on:
            sup = _on
    if not sup:
        return None
    # Cross-chain address collision guard: the exact same literal address can
    # exist as a completely unrelated pair on more than one EVM chain (vanity/
    # CREATE2 deployments, or plain coincidence). Sorting by liquidity alone
    # in that case can silently lock onto a totally different, much bigger
    # project on another chain - e.g. a fresh 435 MC KOL call getting matched
    # to an unrelated $1.5M pool elsewhere that happens to share this address.
    # A freshly-posted call is virtually always the NEWEST pool, so when
    # multiple different chains match, narrow down to the chain whose pool
    # was created most recently before ranking by liquidity within it.
    chains_seen = {p.get("chainId", "").lower() for p in sup}
    if len(chains_seen) > 1:
        dated = [p for p in sup if p.get("pairCreatedAt")]
        if dated:
            newest_chain = max(dated, key=lambda p: p.get("pairCreatedAt") or 0).get("chainId", "").lower()
            same_chain = [p for p in sup if p.get("chainId", "").lower() == newest_chain]
            if same_chain and len(same_chain) < len(sup):
                logger.warning(
                    f"⚠️ Cross-chain address collision for {token_address[:14]}… "
                    f"across {sorted(chains_seen)} - restricting to newest pool's "
                    f"chain ({newest_chain}) to avoid tracking the wrong project")
                sup = same_chain
    sup = sorted(sup, key=lambda p: p.get("liquidity",{}).get("usd",0) or 0, reverse=True)
    best = sup[0]
    chain  = SUPPORTED_CHAINS[best.get("chainId","").lower()]
    price  = float(best.get("priceUsd") or 0)
    mc, mc_is_fdv = _pick_market_cap(sup)
    symbol = best.get("baseToken",{}).get("symbol","")
    if price <= 0 and mc <= 0 and not symbol:
        return None
    price_change_h1  = float((best.get("priceChange") or {}).get("h1") or 0)
    price_change_h24 = float((best.get("priceChange") or {}).get("h24") or 0)
    volume_h24       = float((best.get("volume") or {}).get("h24") or 0)
    liquidity_usd    = float((best.get("liquidity") or {}).get("usd") or 0)
    txns_h24_buys    = int((best.get("txns") or {}).get("h24", {}).get("buys") or 0)
    txns_h24_sells   = int((best.get("txns") or {}).get("h24", {}).get("sells") or 0)
    listed_at        = best.get("pairCreatedAt")
    socials = best.get("info",{}).get("socials") or []
    tg_link = ""
    for s in socials:
        if "t.me" in (s.get("url","")) or "telegram" in (s.get("type","")).lower():
            tg_link = s.get("url",""); break
    return {
        "chain":           chain,
        "mcap":            mc,
        "mcap_fmt":        fmt_mc(mc) if mc > 0 else "N/A",
        "price":           price,
        "symbol":          symbol,
        "name":            best.get("baseToken",{}).get("name",""),
        "pair_addr":       best.get("pairAddress",""),
        "tg_link":         tg_link,
        "price_change_h1": price_change_h1,
        "price_change_h24":price_change_h24,
        "volume_h24":      volume_h24,
        "liquidity_usd":   liquidity_usd,
        "txns_buys":       txns_h24_buys,
        "txns_sells":      txns_h24_sells,
        "listed_at":       listed_at,
        "_partial":        (price <= 0 and mc <= 0),
        "_mc_is_fdv":      bool(mc_is_fdv and mc > 0),
    }


_dex_session = requests.Session()


_dex_miss_cache: dict = {}


_DEX_MISS_TTL = 3           # seconds - short enough for fresh pairs, long enough to avoid storms


_DEX_CACHE_MAX = 4000       # hard cap so long-running Railway deploys never OOM


_DEX_429_UNTIL = 0.0


_DEX_429_COOLDOWN = int(os.environ.get("DEX_429_COOLDOWN_SECONDS", "20") or 20)


def _dex_breaker_open() -> bool:
    return time.time() < _DEX_429_UNTIL


def _dex_trip_breaker():
    global _DEX_429_UNTIL
    _DEX_429_UNTIL = time.time() + _DEX_429_COOLDOWN


_DEX_RATE_LOCK = _threading.Lock()


_DEX_RATE_TIMESTAMPS = deque()


_DEX_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("DEX_MAX_REQUESTS_PER_MINUTE", "260") or 260))


_DEX_STATS = {"ok": 0, "real429": 0, "budget_denied": 0, "breaker_denied": 0, "neterr": 0,
              "bulk_ok": 0, "single_ok": 0, "mon_ok": 0,
              "bulk_denied": 0, "single_denied": 0, "mon_denied": 0}


_DEX_STATS_AT = time.time()


_DEX_EFF_BUDGET = float(_DEX_MAX_REQUESTS_PER_MINUTE)


_DEX_LAST_429_AT = 0.0


_DEX_LAST_DEC_AT = 0.0


_DEX_LAST_ADJ_AT = time.time()


_DEX_BUDGET_FLOOR = 60


_DEX_ADAPTIVE = (os.environ.get("DEX_ADAPTIVE_BUDGET", "0") or "0").strip() == "1"


def _dex_stat(key: str):
    _DEX_STATS[key] = _DEX_STATS.get(key, 0) + 1


def _dex_stats_maybe_log():
    global _DEX_STATS_AT
    now = time.time()
    if now - _DEX_STATS_AT < 60:
        return
    snap = dict(_DEX_STATS)
    for k in list(_DEX_STATS):
        _DEX_STATS[k] = 0
    _DEX_STATS_AT = now
    logger.info(
        f"📊 DexScreener last min: ok={snap.get('ok',0)} real_429={snap.get('real429',0)} "
        f"budget_denied={snap.get('budget_denied',0)} breaker_denied={snap.get('breaker_denied',0)} "
        f"net_err={snap.get('neterr',0)} | budget={int(_DEX_EFF_BUDGET)}/{_DEX_MAX_REQUESTS_PER_MINUTE} "
        f"| lanes ok bulk={snap.get('bulk_ok',0)} single={snap.get('single_ok',0)} mon={snap.get('mon_ok',0)} "
        f"denied bulk={snap.get('bulk_denied',0)} single={snap.get('single_denied',0)} mon={snap.get('mon_denied',0)} "
        f"cooldown={_DEX_429_COOLDOWN}s adaptive={'on' if _DEX_ADAPTIVE else 'off'}")


def _dex_effective_budget() -> int:
    global _DEX_EFF_BUDGET, _DEX_LAST_ADJ_AT
    now = time.time()
    if (_DEX_EFF_BUDGET < _DEX_MAX_REQUESTS_PER_MINUTE
            and now - _DEX_LAST_429_AT > 60 and now - _DEX_LAST_ADJ_AT > 30):
        _DEX_EFF_BUDGET = min(float(_DEX_MAX_REQUESTS_PER_MINUTE),
                              _DEX_EFF_BUDGET + max(5.0, _DEX_MAX_REQUESTS_PER_MINUTE * 0.05))
        _DEX_LAST_ADJ_AT = now
    return int(_DEX_EFF_BUDGET)


def _dex_on_real_429():
    """Called when DexScreener itself returned HTTP 429."""
    global _DEX_EFF_BUDGET, _DEX_LAST_429_AT, _DEX_LAST_DEC_AT
    now = time.time()
    _DEX_LAST_429_AT = now
    if not _DEX_ADAPTIVE:
        return   # observe only - never lowers the request budget
    # several in-flight requests can 429 together - cut only once per 10s
    if now - _DEX_LAST_DEC_AT >= 10:
        _DEX_LAST_DEC_AT = now
        _DEX_EFF_BUDGET = max(float(_DEX_BUDGET_FLOOR), _DEX_EFF_BUDGET * 0.8)


_DEX_LANE_CV = _contextvars.ContextVar("dex_lane", default="single")


_DEX_LANE_TS = {"bulk": deque(), "single": deque(), "mon": deque()}


_DEX_MON_SHARE = float(os.environ.get("DEX_MON_SHARE", "0.25") or 0.25)


def _dex_rate_budget_ok(lane: str = None) -> bool:
    now = time.time()
    lane = lane or _DEX_LANE_CV.get()
    if lane not in _DEX_LANE_TS:
        lane = "single"
    budget = _dex_effective_budget()
    with _DEX_RATE_LOCK:
        for dq in _DEX_LANE_TS.values():
            while dq and now - dq[0] > 60:
                dq.popleft()
        total = sum(len(dq) for dq in _DEX_LANE_TS.values())
        if total >= budget:
            _dex_stat(lane + "_denied")
            return False
        # naye call ka instant track ("single" lane) ke liye ~12% budget HAMESHA reserved:
        # bulk/mon lanes use kabhi nahi chhoo sakte, taake dropped-call speed par asar na pade.
        if lane in ("bulk", "mon") and total >= budget - max(40, int(budget * 0.14)):   # 2026-10-26: 40 slots naye call ke liye reserved
            _dex_stat(lane + "_denied")
            return False
        if lane == "mon" and len(_DEX_LANE_TS["mon"]) >= max(8, int(budget * _DEX_MON_SHARE)):
            _dex_stat("mon_denied")
            return False
        _DEX_LANE_TS[lane].append(now)
        _DEX_RATE_TIMESTAMPS.append(now)          # purana counter (compat)
        while _DEX_RATE_TIMESTAMPS and now - _DEX_RATE_TIMESTAMPS[0] > 60:
            _DEX_RATE_TIMESTAMPS.popleft()
        _dex_stat(lane + "_ok")
        return True


_BIRDEYE_RATE_LOCK = _threading.Lock()


_BIRDEYE_RATE_TIMESTAMPS = deque()


_BIRDEYE_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("BIRDEYE_MAX_REQUESTS_PER_MINUTE", "50") or 50))


def _birdeye_rate_budget_ok() -> bool:
    now = time.time()
    with _BIRDEYE_RATE_LOCK:
        while _BIRDEYE_RATE_TIMESTAMPS and now - _BIRDEYE_RATE_TIMESTAMPS[0] > 60:
            _BIRDEYE_RATE_TIMESTAMPS.popleft()
        if len(_BIRDEYE_RATE_TIMESTAMPS) >= _BIRDEYE_MAX_REQUESTS_PER_MINUTE:
            return False
        _BIRDEYE_RATE_TIMESTAMPS.append(now)
        return True


def _dex_cache_gc():
    """Evict expired / overflowing cache entries (prevents unbounded RAM growth)."""
    import time as _time
    now = _time.time()
    for cache in (_dex_cache, _dex_miss_cache):
        if len(cache) < _DEX_CACHE_MAX:
            continue
        for k in [k for k, v in list(cache.items())
                  if (v[1] if isinstance(v, tuple) else v) < now]:
            cache.pop(k, None)
        while len(cache) > _DEX_CACHE_MAX:
            try:
                cache.pop(next(iter(cache)))
            except StopIteration:
                break


def _dex_get(url: str, timeout: int = 5, fresh: bool = False, lane: str = None):
    """GET with shared session. fresh=True bypasses intermediary HTTP caches."""
    try:
        _dex_stats_maybe_log()
    except Exception:
        pass
    if _dex_breaker_open():
        _dex_stat("breaker_denied")
        return 429  # breaker open: request bhejne ke baghair rate-limited batao
    if not _dex_rate_budget_ok(lane):
        _dex_stat("budget_denied")
        return 429  # apna hi budget khatam - DexScreener ko hit kiye baghair fallback pe jao
    try:
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"} if fresh else None
        if fresh:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}_ts={int(time.time() * 1000)}"
        resp = _dex_session.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        _dex_stat("neterr")
        logger.warning(f"DexScreener network error ({url.split('/')[-1][:16]}…): {e}")
        return None
    if resp.status_code == 429:
        _dex_stat("real429")
        _dex_on_real_429()
        _dex_trip_breaker()
        return 429
    if resp.status_code != 200:
        return None
    _dex_stat("ok")
    try:
        return resp.json()
    except Exception:
        return None


def _dex_candidate_chains(ca: str) -> list:
    """Which DexScreener chains can this address exist on? Keeps the
    chain-scoped lookup to 1-4 cheap requests instead of brute-forcing."""
    # len==42: standard EVM (0x+40hex). len==66: Robinhood/"RH" 32-byte
    # address format (0x+64hex) seen on DexScreener chart links - without
    # this, a correctly-extracted Robinhood CA still failed here and fell
    # through to being queried as if it were a Solana address.
    if ca.startswith("0x") and len(ca) in (42, 66):
        return ["ethereum", "bsc", "base", "robinhood"]
    if ca.startswith(("EQ", "UQ")):
        return ["ton"]
    return ["solana"]


_pair_token_cache: dict = {}


_PAIR_RESOLVE_NEG_TTL = 90  # seconds - brand-new pairs aren't indexed by


def _resolve_pair_to_token(addr: str):
    """Return the baseToken mint for a pair/pool address, else None."""
    if not addr:
        return None
    cached = _pair_token_cache.get(addr)
    if cached is not None:
        # WIZCHAIN_NOV23: cache is (token, chain_slug, expires_at); old
        # 2-tuple entries (token, expires_at) are still honoured.
        if len(cached) == 3:
            token_c, chain_c, expires_at = cached
        else:
            token_c, expires_at = cached
            chain_c = None
        if token_c or time.time() < expires_at:
            return (token_c, chain_c)
        # negative result expired - fall through and try again
    token = None
    pair_slug = None  # WIZCHAIN_NOV23: DexScreener slug where the pair lives
    try:
        for slug in (_dex_candidate_chains(addr) or []):
            d = _dex_get(f"https://api.dexscreener.com/latest/dex/pairs/{slug}/{addr}",
                         timeout=8)
            if d == 429:
                break
            if not isinstance(d, dict):
                continue
            prs = d.get("pairs") or ([d.get("pair")] if d.get("pair") else [])
            for p in prs:
                if not isinstance(p, dict):
                    continue
                base = ((p.get("baseToken") or {}).get("address") or "").strip()
                if base and base.lower() != addr.lower():
                    token = base
                    pair_slug = slug  # WIZCHAIN_NOV23: remember the pair's chain
                    break
            if token:
                break
    except Exception as e:
        logger.debug(f"pair→token resolve failed {addr[:12]}…: {e}")
    # FIX (2026-09-25, maybac_whales RH pair-link calls stuck dex_pending
    # forever incident): logs showed several Robinhood-chain calls where the
    # KOL only posted a dexscreener.com/robinhood/<pairAddress> link (no
    # separate contract line) sitting at dex_pending=True for 30-60+ minutes
    # straight and never resolving - silently excluded from tracking,
    # Leaderboard and Champions the whole time even though other chains'
    # pair-links resolved in seconds. DexScreener's OWN /latest/dex/pairs/
    # <chain>/<addr> endpoint (above) can badly lag or never index a
    # brand-new Robinhood-chain pool. GeckoTerminal indexes the "robinhood"
    # network independently (already relied on elsewhere in this file for
    # RH-chain cross-checks) and exposes the base token on a plain pool
    # lookup, so it is now tried as a second, independent resolver instead
    # of leaving the call stuck forever on DexScreener alone.
    if not token:
        _GT_PAIR_SLUG = {"robinhood": "robinhood", "solana": "solana", "ethereum": "eth",
                         "bsc": "bsc", "base": "base", "ton": "ton", "arc": "arc"}
        try:
            for slug in (_dex_candidate_chains(addr) or []):
                net = _GT_PAIR_SLUG.get(slug)
                if not net:
                    continue
                gd = _gt_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{addr}")
                base_id = ((((gd or {}).get("data") or {}).get("relationships") or {})
                           .get("base_token") or {}).get("data", {}).get("id", "") or ""
                if "_" in base_id:
                    base_addr = base_id.split("_", 1)[1].strip()
                    if base_addr and base_addr.lower() != addr.lower():
                        token = base_addr
                        pair_slug = slug  # WIZCHAIN_NOV23: remember the pair's chain
                        logger.info(f"🔗 Pair address resolved via GeckoTerminal (DexScreener "
                                    f"had no pool data): {addr[:10]}… → token {token[:10]}…")
                        break
        except Exception as e:
            logger.debug(f"pair→token GT resolve failed {addr[:12]}…: {e}")
    if len(_pair_token_cache) > 4000:
        _pair_token_cache.clear()
    # Positive resolves are permanent (a pair's base token never changes).
    # Negative resolves expire quickly so a too-early attempt gets retried
    # once DexScreener/GeckoTerminal actually indexes the pair.
    # WIZCHAIN_NOV23: cache now stores (token, chain_slug, expires_at).
    _pair_token_cache[addr] = (token, pair_slug, time.time() + _PAIR_RESOLVE_NEG_TTL)
    return (token, pair_slug)


BIRDEYE_API_KEY = os.environ.get("BIRDEYE_API_KEY", "").strip()


MORALIS_API_KEY = os.environ.get("MORALIS_API_KEY", "").strip()


CMC_API_KEY     = (os.environ.get("CMC_API_KEY", "")
                   or os.environ.get("COINMARKETCAP_API_KEY", "")).strip()


EXTRA_SOURCES_ENABLED = os.environ.get("EXTRA_SOURCES", "1").strip().lower() not in ("0", "false", "off")


_GT_NET   = {"SOL": "solana", "ETH": "eth", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}


_DP_NET   = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}


_BE_CHAIN = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base"}


_MO_EVM   = {"ETH": "eth", "BNB": "bsc", "BASE": "base", "ARC": "arc"}


_SLUG_TO_DISP = {"solana": "SOL", "ethereum": "ETH", "eth": "ETH", "bsc": "BNB",
                 "base": "BASE", "ton": "TON", "robinhood": "RH", "arc": "ARC"}


def _alt_result(chain, symbol, price, mcap, name="", source="", mc_is_fdv=False):
    """Build a dict in the exact shape _parse_dex_pairs() returns.

    mc_is_fdv: True when `mcap` was only obtainable via a fully-diluted-value
    fallback (no real circulating marketCap field was available from this
    provider). FDV is computed off max/total supply, not circulating supply,
    and can read 10-100x+ higher than the token's real market cap for tokens
    with large locked/uncirculated allocations - this is what caused fresh
    listings (e.g. Robinhood-chain tokens) to show as millions in MC when the
    caller's real entry was only thousands. Callers that need entry-MC
    accuracy (e.g. _reconcile_entry_mc) use this flag to avoid trusting an
    FDV-derived number over the KOL's own stated MC.
    """
    try:
        price = float(price or 0)
    except Exception:
        price = 0.0
    try:
        mcap = float(mcap or 0)
    except Exception:
        mcap = 0.0
    if price <= 0 and mcap <= 0 and not symbol:
        return None
    dexpath = CHAIN_TO_DEXPATH.get(chain, "solana") if "CHAIN_TO_DEXPATH" in globals() else "solana"
    return {
        "chain":            chain,
        "mcap":             mcap,
        "mcap_fmt":         fmt_mc(mcap) if mcap > 0 else "N/A",
        "price":            price,
        "symbol":           symbol or "",
        "name":             name or symbol or "",
        "pair_addr":        "",
        "tg_link":          "",
        "price_change_h1":  0.0,
        "price_change_h24": 0.0,
        "volume_h24":       0.0,
        "liquidity_usd":    0.0,
        "txns_buys":        0,
        "txns_sells":       0,
        "listed_at":        None,
        "_partial":         (price <= 0 and mcap <= 0),
        "_source":          source or "alt",
        "_dexpath":         dexpath,
        "_mc_is_fdv":       bool(mc_is_fdv and mcap > 0),
    }


def _alt_chains_for(ca: str) -> list:
    """Display-chain candidates for a contract address."""
    return [_SLUG_TO_DISP.get(s, "SOL") for s in _dex_candidate_chains(ca)
            if s in _SLUG_TO_DISP]


def _src_geckoterminal(ca: str, chains: list):
    for ch in chains:
        net = _GT_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            a = (r.json().get("data") or {}).get("attributes") or {}
            price   = float(a.get("price_usd") or 0)
            mc_real = float(a.get("market_cap_usd") or 0)
            mc_fdv  = float(a.get("fdv_usd") or 0)
            mc      = mc_real or mc_fdv
            res   = _alt_result(ch, a.get("symbol") or "", price, mc,
                                a.get("name") or "", "geckoterminal",
                                mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"GeckoTerminal source failed ({ch}): {e}")
    return None


def _src_dexpaprika(ca: str, chains: list):
    for ch in chains:
        net = _DP_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.dexpaprika.com/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            d  = r.json() or {}
            sm = d.get("summary") or {}
            price   = float(sm.get("price_usd") or d.get("price_usd") or 0)
            mc_real = float(d.get("market_cap_usd") or sm.get("market_cap_usd") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "dexpaprika",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"DexPaprika source failed ({ch}): {e}")
    return None


def _src_birdeye(ca: str, chains: list):
    if not BIRDEYE_API_KEY:
        return None
    for ch in chains:
        bchain = _BE_CHAIN.get(ch)
        if not bchain:
            continue
        if not _birdeye_rate_budget_ok():
            continue  # account-wide budget used up this minute - skip, try next chain/next tick
        try:
            r = _dex_session.get(
                "https://public-api.birdeye.so/defi/token_overview",
                params={"address": ca},
                headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": bchain,
                         "accept": "application/json"},
                timeout=8)
            if r.status_code != 200:
                continue
            d = (r.json() or {}).get("data") or {}
            if not d:
                continue
            price   = float(d.get("price") or 0)
            mc_real = float(d.get("marketCap") or d.get("mc") or d.get("realMc") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "birdeye",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"Birdeye source failed ({ch}): {e}")
    return None


def _src_moralis(ca: str, chains: list):
    if not MORALIS_API_KEY:
        return None
    hdr = {"X-API-Key": MORALIS_API_KEY, "accept": "application/json"}
    for ch in chains:
        try:
            if ch == "SOL":
                r = _dex_session.get(
                    f"https://solana-gateway.moralis.io/token/mainnet/{ca}/price",
                    headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = ""
                mc    = 0.0
                try:
                    rm = _dex_session.get(
                        f"https://solana-gateway.moralis.io/token/mainnet/{ca}/metadata",
                        headers=hdr, timeout=8)
                    if rm.status_code == 200:
                        m   = rm.json() or {}
                        sym = m.get("symbol") or ""
                        supply = float(m.get("supply") or 0)
                        dec    = int(m.get("decimals") or 0) if str(m.get("decimals") or "").isdigit() else 0
                        if supply > 0 and price > 0:
                            if dec and supply > 10 ** 12:
                                supply = supply / (10 ** dec)
                            mc = supply * price
                except Exception:
                    pass
                res = _alt_result("SOL", sym, price, mc, sym, "moralis")
                if res:
                    return res
            else:
                evm = _MO_EVM.get(ch)
                if not evm:
                    continue
                r = _dex_session.get(
                    f"https://deep-index.moralis.io/api/v2.2/erc20/{ca}/price",
                    params={"chain": evm}, headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = (d.get("tokenSymbol") or "")
                res = _alt_result(ch, sym, price, 0, d.get("tokenName") or sym, "moralis")
                if res:
                    return res
        except Exception as e:
            logger.debug(f"Moralis source failed ({ch}): {e}")
    return None


def _src_coinmarketcap(ca: str, chains: list):
    if not CMC_API_KEY:
        return None
    hdr = {"X-CMC_PRO_API_KEY": CMC_API_KEY, "Accept": "application/json"}
    try:
        r = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info",
            params={"address": ca}, headers=hdr, timeout=10)
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get("data") or {}
        if not data:
            return None
        cid, info = next(iter(data.items()))
        if isinstance(info, list):
            info = info[0] if info else {}
        sym  = info.get("symbol") or ""
        name = info.get("name") or ""
        rq = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest",
            params={"id": cid}, headers=hdr, timeout=10)
        price = mc = 0.0
        if rq.status_code == 200:
            q = ((rq.json() or {}).get("data") or {}).get(str(cid)) or {}
            if isinstance(q, list):
                q = q[0] if q else {}
            usd   = (q.get("quote") or {}).get("USD") or {}
            price = float(usd.get("price") or 0)
            mc    = float(usd.get("market_cap") or usd.get("fully_diluted_market_cap") or 0)
        ch = (chains or ["SOL"])[0]
        return _alt_result(ch, sym, price, mc, name, "coinmarketcap")
    except Exception as e:
        logger.debug(f"CoinMarketCap source failed: {e}")
    return None


def _fetch_alt_sources_sync(ca: str):
    """DexScreener ke fail hone par baaki sources try karo (order = fastest first)."""
    if not ca or not EXTRA_SOURCES_ENABLED:
        return None
    chains = _alt_chains_for(ca) or ["SOL"]
    # Birdeye first ONLY for chain-unambiguous addresses (SOL/TON) - it's a
    # paid, independently-indexed source and far more reliable than the free
    # geckoterminal/dexpaprika endpoints (which often return mc=N/A for
    # fresh/low-cap tokens). For EVM (0x...) addresses the chain itself is a
    # guess across ETH/BSC/Base/Robinhood, and Birdeye just takes the first
    # chain with any data - which can be a wrong/unrelated token on a
    # different chain with a very different MC. So for those, try the
    # chain-neutral sources first and treat Birdeye as a last resort there.
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    order = ((_src_geckoterminal, _src_dexpaprika, _src_birdeye,
              _src_moralis, _src_coinmarketcap) if is_evm_ambiguous else
             (_src_birdeye, _src_geckoterminal, _src_dexpaprika,
              _src_moralis, _src_coinmarketcap))
    # SPEED_NOV12: all alt sources start at once (was: strictly sequential,
    # so a slow first source blocked every later one - up to 5 x 8s = 40s).
    # Results are still taken in the same priority order: the first source
    # (by priority) with data wins, exactly as before. Only the wall time
    # changes, capped by a shared 10s deadline. Stragglers are left to
    # finish in the background (shutdown(wait=False)) so an early hit is
    # never delayed by a slow lower-priority source.
    try:
        import concurrent.futures as _cf
        _deadline = time.time() + 10
        _ex = _cf.ThreadPoolExecutor(max_workers=len(order))
        try:
            _futs = [_ex.submit(fn, ca, chains) for fn in order]
            for _fn, _f in zip(order, _futs):
                try:
                    res = _f.result(timeout=max(0.1, _deadline - time.time()))
                except Exception as e:
                    logger.debug(f"alt source {getattr(_fn, '__name__', '?')} error: {e}")
                    res = None
                if res and (res.get("price", 0) > 0 or res.get("mcap", 0) > 0 or res.get("symbol")):
                    logger.info(f"🛰️ Fallback source {res.get('_source')} hit for {ca[:12]}… "
                                f"({res.get('symbol','?')} mc={res.get('mcap_fmt','N/A')})")
                    return res
        finally:
            _ex.shutdown(wait=False)
    except Exception as e:
        logger.debug(f"alt sources parallel run failed: {e}")
    return None


def _better_mc_result(a: dict | None, b: dict | None) -> dict | None:
    """Pick the more trustworthy of two dex-shaped MC reads.

    A real (non-FDV) marketCap always beats an FDV-derived one, regardless of
    which is numerically higher - FDV can be 10-100x+ inflated relative to
    real circulating MC. Within the same tier (both real, or both FDV-only),
    the higher reading wins: indexing lag only ever biases a read LOW, never
    high, so taking the higher of two same-tier reads can never inflate the
    caller's own X in their favor.
    """
    a_mc = (a or {}).get("mcap", 0) or 0
    b_mc = (b or {}).get("mcap", 0) or 0
    if a_mc <= 0:
        return b
    if b_mc <= 0:
        return a
    a_fdv = bool((a or {}).get("_mc_is_fdv"))
    b_fdv = bool((b or {}).get("_mc_is_fdv"))
    if a_fdv and not b_fdv:
        return b
    if b_fdv and not a_fdv:
        return a
    return a if a_mc >= b_mc else b


def _confirm_mc_cross_check(ca: str, chain_guess: str = "SOL") -> dict | None:
    """Re-check a token's MC against an INDEPENDENT provider, not just DexScreener
    asked twice. DexScreener's own indexer can still be settling well after our
    first read, so re-querying only DexScreener risks repeating the exact same
    stale (or FDV-only) number. Returns a dex-shaped dict (mcap/price/symbol)
    or None.

    EVM (0x...) addresses are chain-ambiguous (same address format valid on
    ETH/BSC/Base/Robinhood) - a chain-guessing alt source just returns the
    first chain with any data for that address, which can be a totally
    unrelated token on a different chain with a wildly different MC.
    DexScreener resolves the chain correctly (real liquidity/pairs), so for
    EVM addresses its number is trusted UNLESS it has nothing real to offer
    either - no data at all, OR only an FDV fallback (see _pick_market_cap),
    which is exactly the case this cross-check exists to correct.

    FIX (RH-chain cross-check no-op, 2026-09-24): Birdeye (_BE_CHAIN) has no
    entry at all for "RH" - Birdeye simply does not index the Robinhood
    chain - so `be_res` was always None for every Robinhood-chain call, and
    this whole function silently did nothing but re-ask DexScreener the same
    question it had just asked. GeckoTerminal DOES index the "robinhood"
    network (_GT_NET / _GECKO_NET), so it is now tried as well whenever
    Birdeye comes back empty, giving RH-chain calls an actual independent
    second opinion for the first time.
    """
    dex_res = None
    alt_res = None
    # FIX (2026-10-11, CENTRY "278K shown, real 400K"): while DexScreener was rate-limited
    # (429 breaker open) _fetch_dex_sync() silently answered from its alt sources - mostly a
    # lagging GeckoTerminal quote - and this function then trusted that number as
    # "real, chain-resolved DexScreener MC", so the cross-check just confirmed the stale value.
    # allow_alt=False => dex_res is ONLY ever a genuine DexScreener answer (None under 429).
    # SPEED_NOV12: DexScreener + Birdeye + GeckoTerminal now run concurrently
    # (was: DexScreener first, then the two alts in parallel). Same three
    # sources, same selection logic below (incl. the EVM live-entry guard) -
    # only the wall time changes: a read costs max(source times) instead of
    # dex + max(alt times).
    _alt_cands = []
    try:
        import concurrent.futures as _cf
        _chs = _alt_chains_for(ca) or [chain_guess]
        with _cf.ThreadPoolExecutor(max_workers=3) as _ex:
            _f_dex = _ex.submit(_fetch_dex_sync, ca, 1, 0, False)
            _f_be = _ex.submit(_src_birdeye, ca, _chs)
            _f_gt = _ex.submit(_src_geckoterminal, ca, _chs)
            try:
                dex_res = _f_dex.result()
            except Exception:
                dex_res = None
            for _f in (_f_be, _f_gt):
                try:
                    _r = _f.result(timeout=1)  # GUARD_2SEC_NOV19
                    if _r:
                        _alt_cands.append(_r)
                except Exception:
                    pass
    except Exception:
        pass
    # Provider lag only ever biases a quote LOW, so of the independent alt sources keep the
    # highest real (non-FDV) MC; fall back to any non-empty answer.
    _real = [r for r in _alt_cands if (r.get("mcap", 0) or 0) > 0 and not r.get("_mc_is_fdv")]
    if _real:
        alt_res = max(_real, key=lambda r: r.get("mcap", 0) or 0)
    else:
        alt_res = next((r for r in _alt_cands if (r.get("mcap", 0) or 0) > 0), None) or (_alt_cands[0] if _alt_cands else None)
    dex_mc = (dex_res or {}).get("mcap", 0) or 0
    dex_is_fdv = bool((dex_res or {}).get("_mc_is_fdv"))
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    if is_evm_ambiguous:
        if dex_mc > 0 and not dex_is_fdv:
            # LIVE_ENTRY_GUARD_NOV12: DexScreener can still be stale/LOW for
            # a fresh EVM pair (e.g. $JAGENT locked 1.4K when the real call
            # MC was 22K). Before trusting its real MC outright, check the
            # independent alt sources for the SAME chain AND SAME symbol.
            # A wrong-chain alt (same 0x address, unrelated token) must
            # never inflate the entry, so both must match; otherwise the
            # DexScreener answer stands (fail closed).
            _dex_chain = (dex_res or {}).get("chain") or chain_guess
            _dex_sym = str((dex_res or {}).get("symbol") or "").strip().upper()
            _best_alt = None
            for _cand in (_alt_cands or []):
                _c_mc = (_cand or {}).get("mcap", 0) or 0
                if _c_mc <= 0 or bool((_cand or {}).get("_mc_is_fdv")):
                    continue  # only a real (non-FDV) alt may override
                if str((_cand or {}).get("chain") or "") != str(_dex_chain or ""):
                    continue  # wrong chain - unrelated token, ignore
                _c_sym = str((_cand or {}).get("symbol") or "").strip().upper()
                if not _dex_sym or not _c_sym or _c_sym != _dex_sym:
                    continue  # symbol must match on both sides
                if _best_alt is None or _c_mc > (_best_alt.get("mcap", 0) or 0):
                    _best_alt = _cand
            if _best_alt is not None:
                _alt_mc = (_best_alt.get("mcap", 0) or 0)
                if _alt_mc > dex_mc * 2:
                    logger.info(
                        f"Live-entry guard: DexScreener MC {fmt_mc(dex_mc)} looks stale "
                        f"vs {_best_alt.get('_source', 'alt')} {fmt_mc(_alt_mc)} "
                        f"(chain={_dex_chain} symbol={_dex_sym}) - using the higher read")
                    return _best_alt
            # ENTRY_DUST_NOV20 ($OWED: entry locked $3 while the token traded
            # at $31K): DexScreener can lag minutes behind on a fresh pair and
            # report the pre-pump dust price as a real MC. Prices are
            # comparable across sources without supply assumptions, so if an
            # independent alt source reports a price >2x away from the
            # DexScreener price for the SAME chain+symbol, the Dex read is
            # stale - flag it so callers treat it as unconfirmed instead of
            # locking it in as a confident entry. The alt MC is NOT
            # substituted here (it may be FDV-derived); only the flag is set.
            _dex_price = float((dex_res or {}).get("price", 0) or 0)
            for _cand in (_alt_cands or []):
                if str((_cand or {}).get("chain") or "") != str(_dex_chain or ""):
                    continue
                _c_sym = str((_cand or {}).get("symbol") or "").strip().upper()
                if not _dex_sym or not _c_sym or _c_sym != _dex_sym:
                    continue
                _c_price = float((_cand or {}).get("price", 0) or 0)
                if _dex_price > 0 and _c_price > 0 and max(_dex_price, _c_price) / min(_dex_price, _c_price) > 2:
                    logger.warning(
                        f"Dust-price guard: DexScreener price {_dex_price} disagrees "
                        f"with {_cand.get('_source', 'alt')} price {_c_price} "
                        f"(chain={_dex_chain} symbol={_dex_sym}) - flagging read stale")
                    _stale = dict(dex_res)
                    _stale["_price_stale"] = True
                    return _stale
            return dex_res  # real, chain-resolved circulating MC - trust it outright
        return _better_mc_result(alt_res, dex_res)
    return _better_mc_result(dex_res, alt_res)


def _fetch_dex_sync(ca, retries=2, min_liquidity=500, allow_alt=True, prefer_chain=None):
    """Fetch live token data from DexScreener.

    Order of operations:
      1. Fresh positive cache hit (< 20s)  -> return instantly
      2. Fresh negative cache hit (< 45s)  -> return None without a request
      3. /latest/dex/tokens/{ca}           -> primary, all chains
      4. /latest/dex/search?q={ca}         -> fallback for freshly-created pairs
      5. 429 -> exponential backoff, then retry
    """
    import time as _time
    if not ca:
        return None

    cached = _dex_cache.get(ca)
    if cached:
        result, exp = cached
        if _time.time() < exp:
            return result

    miss_exp = _dex_miss_cache.get(ca)
    if miss_exp and _time.time() < miss_exp:
        return None

    for attempt in range(max(1, retries)):
        data = _dex_get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
        if data == 429:
            # Rate-limited (breaker _dex_get me trip ho chuka hai). DexScreener
            # ko sleep kar ke dobara MAT bajao - wahi backoff storms thay jo
            # ticks wedge kar ke calls skip kara rahe thay. Seedha doosri dex
            # sources se data lo.
            logger.warning(f"DexScreener 429 (ca={ca[:12]}…) - alt dex sources try ho rahi hain")
            alt429 = _fetch_alt_sources_sync(ca) if allow_alt else None
            if alt429:
                _dex_cache[ca] = (alt429, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                _dex_cache_gc()
                return alt429
            # Alt bhi fail - 5s negative cache taake koi storm na bane.
            _dex_miss_cache[ca] = _time.time() + 5
            _dex_cache_gc()
            return None

        pairs = (data or {}).get("pairs") or []

        # Fallback: brand-new pairs sometimes only show on the search endpoint.
        if not pairs:
            sdata = _dex_get(f"https://api.dexscreener.com/latest/dex/search?q={ca}")
            if sdata and sdata != 429:
                pairs = [
                    p for p in (sdata.get("pairs") or [])
                    if ca.lower() in (
                        (p.get("baseToken", {}).get("address", "") or "").lower(),
                        (p.get("quoteToken", {}).get("address", "") or "").lower(),
                    )
                ]

        if pairs:
            result = _parse_dex_pairs(pairs, min_liquidity, ca, prefer_chain=prefer_chain)
            if result is None:
                # Pairs exist but all below the liquidity floor - retry without it
                # so the UI still shows a live price instead of "N/A".
                result = _parse_dex_pairs(pairs, 0, ca, prefer_chain=prefer_chain)
        else:
            result = None

        # The /latest endpoint caps its answer at 30 pairs, so a multi-chain
        # token can come back with zero pairs on a chain we support. Ask the
        # chain-scoped v1 endpoint directly for the chains this CA can live on.
        if not result:
            scoped = []
            for chain_slug in _dex_candidate_chains(ca):
                cdata = _dex_get(f"https://api.dexscreener.com/tokens/v1/{chain_slug}/{ca}", timeout=10)
                if cdata == 429:
                    break
                if isinstance(cdata, list) and cdata:
                    scoped.extend(cdata)
            if scoped:
                result = (_parse_dex_pairs(scoped, min_liquidity, ca, prefer_chain=prefer_chain)
                          or _parse_dex_pairs(scoped, 0, ca, prefer_chain=prefer_chain))

        if True:
            if result:
                _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                _dex_cache_gc()
                return result

        # ── PAIR-ADDRESS FALLBACK ────────────────────────────────────────────
        # Bohat se KOLs (jaise @Big_Whales_Call) dexscreener ka PAIR/POOL link
        # post karte hain: dexscreener.com/solana/<pairAddress>. Wo address
        # token mint nahi hota, is liye /latest/dex/tokens/<addr> khali aata
        # tha aur poori call "no dex data yet" keh kar skip ho jati thi.
        # Yahan us address ko pairs endpoint se resolve kar ke asli baseToken
        # mint nikalte hain, phir usi se normal lookup chalta hai.
        if not result:
            # WIZCHAIN_NOV23: the resolver now returns (token, chain_slug).
            # The pair's chain is the call's chain - pass it down so the
            # token lookup cannot wander off to another chain's pair.
            _tok, _pair_slug = _resolve_pair_to_token(ca)
            if _tok and _tok.lower() != (ca or "").lower():
                logger.info(f"🔗 Pair address resolved: {ca[:10]}… → token {_tok[:10]}…")
                result = _fetch_dex_sync(_tok, retries=1,
                                         min_liquidity=min_liquidity,
                                         allow_alt=allow_alt,
                                         prefer_chain=_pair_slug or prefer_chain)
                if result:
                    result["resolved_ca"] = _tok

        if result:
            _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
            _dex_miss_cache.pop(ca, None)
            _dex_cache_gc()
            return result

        # DexScreener has nothing - try the slower alternate providers only on
        # normal lookup/new-call paths. The 1-second monitoring loop passes
        # allow_alt=False so fallback APIs cannot wedge instant alerts.
        alt = _fetch_alt_sources_sync(ca) if allow_alt else None
        if alt:
            _dex_cache[ca] = (alt, _time.time() + _DEX_CACHE_TTL)
            _dex_miss_cache.pop(ca, None)
            _dex_cache_gc()
            return alt

        # Not indexed anywhere yet -> short negative cache, no more retries this round.
        _dex_miss_cache[ca] = _time.time() + _DEX_MISS_TTL
        _dex_cache_gc()
        return None

    return None


_BIRDEYE_BULK_429_UNTIL = 0.0


_BIRDEYE_BULK_COOLDOWN  = int(os.environ.get("BIRDEYE_BULK_429_COOLDOWN_SECONDS", "15") or 15)


def _bulk_refresh_birdeye_sync(cas: list, chain_map: dict) -> int:
    """DexScreener breaker open hone par bulk batch ko Birdeye multi_price se
    refresh karta hai - /defi/multi_price ek request mein 100 tak addresses
    accept karta hai. chain_map: {ca: 'SOL'|'ETH'|'BNB'|'BASE', ...} - jo
    tracked_calls['chain'] se already pata hai, guess nahi karna padta.
    NOTE: this account's key is the FREE tier (60 req/min total, shared with
    _src_birdeye's single-CA calls) - see _birdeye_rate_budget_ok(), which
    every sub-batch request below checks first so this fallback can't blow
    through the account's own limit and start 429-ing itself.
    """
    import time as _time
    if not BIRDEYE_API_KEY or not cas:
        return 0
    global _BIRDEYE_BULK_429_UNTIL
    if _time.time() < _BIRDEYE_BULK_429_UNTIL:
        return 0
    by_chain: dict = {}
    for ca in cas:
        bchain = _BE_CHAIN.get((chain_map or {}).get(ca, "SOL"), "solana")
        by_chain.setdefault(bchain, []).append(ca)
    updated = 0
    for bchain, addrs in by_chain.items():
        for i in range(0, len(addrs), 100):
            sub = addrs[i:i + 100]
            if not _birdeye_rate_budget_ok():
                logger.debug("Birdeye bulk refresh: account budget used up this minute, skipping rest")
                return updated
            try:
                r = _dex_session.get(
                    "https://public-api.birdeye.so/defi/multi_price",
                    params={"list_address": ",".join(sub)},
                    headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": bchain,
                             "accept": "application/json"},
                    timeout=8)
            except Exception as e:
                logger.debug(f"Birdeye bulk refresh network error: {e}")
                continue
            if r.status_code == 429:
                _BIRDEYE_BULK_429_UNTIL = _time.time() + _BIRDEYE_BULK_COOLDOWN
                logger.warning("Birdeye bulk refresh 429 - cooling down, DexScreener single-CA fallback will cover this tick")
                return updated
            if r.status_code != 200:
                continue
            data = (r.json() or {}).get("data") or {}
            disp_chain = _SLUG_TO_DISP.get(bchain, (chain_map or {}).get(sub[0], "SOL"))
            for addr, d in data.items():
                if not d:
                    continue
                price = float(d.get("value") or d.get("price") or 0)
                if price <= 0:
                    continue
                result = _alt_result(disp_chain, d.get("symbol") or "", price,
                                      float(d.get("marketCap") or d.get("mc") or 0),
                                      d.get("name") or "", "birdeye_bulk")
                if result:
                    # Actual tracked chain (not the display slug) so the caller's
                    # symbol/chain assignment on the call stays correct.
                    result["chain"] = (chain_map or {}).get(addr, disp_chain)
                    _dex_cache[addr] = (result, _time.time() + _DEX_CACHE_TTL)
                    _dex_miss_cache.pop(addr, None)
                    updated += 1
    if updated:
        logger.info(f"🐦 Birdeye bulk refresh filled {updated} token(s) while DexScreener breaker was open")
    return updated


def _bulk_refresh_dex_sync(cas: list, min_liquidity: int = 0, chain_map: dict = None) -> int:
    """Refresh the DexScreener cache for many tokens with few requests.

    /latest/dex/tokens/{a,b,c} accepts up to 30 comma-separated addresses, so
    tracking 300 tokens costs ~10 requests instead of 300. This is what makes
    real-time (5 s) tracking possible without hitting rate limits.
    """
    import time as _time
    uniq, seen_local = [], set()
    for ca in cas or []:
        if ca and ca not in seen_local:
            seen_local.add(ca)
            uniq.append(ca)
    updated = 0
    for i in range(0, len(uniq), 30):
        batch = uniq[i:i + 30]
        data = _dex_get(f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}", fresh=True, lane="bulk")
        if data == 429:
            # Breaker open ho chuka hai - baaki batches bhi 429 hi hongi.
            # Is batch aur baaki sab ko Birdeye se cover karo (paisay already
            # diye hue hain isi ke liye) taake tokens poori tarah stale na
            # rahein jab tak DexScreener cooldown khatam nahi hota.
            remaining = uniq[i:]
            try:
                updated += _bulk_refresh_birdeye_sync(remaining, chain_map or {})
            except Exception as e_be:
                logger.warning(f"Birdeye bulk fallback failed: {e_be}")
            _now_w = _time.time()
            if _now_w - globals().get("_BULK429_LOG_AT", 0.0) >= 10:
                globals()["_BULK429_LOG_AT"] = _now_w
                logger.warning("DexScreener 429 bulk refresh - is tick ke baaki batches skip "
                               "(breaker open / budget khatam; details: '📊 DexScreener last min' line)")
            break
        pairs = (data or {}).get("pairs") or []
        if not pairs:
            continue
        grouped: dict = {}
        wanted = {c.lower(): c for c in batch}
        for p in pairs:
            # DexScreener's priceUsd belongs to baseToken only. Never assign a
            # quote-token pair to a tracked CA (that tracks SOL/WETH by mistake).
            addr = ((p.get("baseToken") or {}).get("address") or "").lower()
            if addr in wanted:
                grouped.setdefault(wanted[addr], []).append(p)
        for ca, plist in grouped.items():
            result = _parse_dex_pairs(plist, min_liquidity, ca)
            if result:
                _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                updated += 1
        if i + 30 < len(uniq):
            _time.sleep(0.08)
    _dex_cache_gc()
    return updated


def _invalidate_dex_cache(ca: str):
    """Force the next fetch to be genuinely fresh.

    IMPORTANT: clear BOTH positive and negative caches. A negative-cache entry
    is what can otherwise make a brand-new caller stay alert_pending for 45s
    even though DexScreener starts returning the token immediately afterwards.
    """
    if not ca:
        return
    _dex_cache.pop(ca, None)
    _dex_miss_cache.pop(ca, None)


def _get_cached_dex(ca: str):
    """Return only the current bulk-refresh result; never perform network I/O.

    The monitoring loop used to call _fetch_dex_sync once per tracked token
    after it had already done a bulk request.  On a cache miss that spawned
    dozens of slow requests (and fallback-provider requests) in the same tick,
    causing the next tick to overlap and making X alerts arrive late.
    """
    if not ca:
        return None
    cached = _dex_cache.get(ca)
    if not cached:
        return None
    result, expiry = cached
    if time.time() >= expiry:
        return None
    return result


async def fetch_dexscreener(ca, *, allow_alt=True, retries=2, min_liquidity=500):
    return await asyncio.to_thread(_fetch_dex_sync, ca, retries, min_liquidity, allow_alt)


def _gt_get(url, timeout=12):
    try:
        r = _dex_session.get(url, headers={"Accept": "application/json"}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.debug(f"gecko get failed {url}: {e}")
    return None


DIP_MILESTONES_BASE = [2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50,
                       60, 70, 80, 90, 100, 150, 200, 250, 500, 1000, 1500]


DIP_MAX_X = 10000


DIP_DEFAULT_THRESHOLD_PCT = 30.0


DIP_DUST_FLOOR_FRAC = 0.01  # DIP_MAX99_NOV23: max dip 99% (low must stay >= 1% of entry)


DIP_LOW_CONFIRM_TICKS = 1  # DIPCONFIRM1_NOV23: V-shape lows confirm on first sight


DIP_RESET_BELOW_FRAC = 0.8


def _dip_cfg():
    try:
        return load_config()
    except Exception:
        return {}


def dip_enabled():
    try:
        return bool(_dip_cfg().get("dip_enabled", True))
    except Exception:
        return True


def dip_threshold_pct():
    try:
        v = float(_dip_cfg().get("dip_threshold_pct", DIP_DEFAULT_THRESHOLD_PCT)
                  or DIP_DEFAULT_THRESHOLD_PCT)
    except (TypeError, ValueError):
        v = DIP_DEFAULT_THRESHOLD_PCT
    return min(max(v, 1.0), 50.0)


def dip_milestones_for_ratio(ratio):
    try:
        ratio = float(ratio or 0)
    except (TypeError, ValueError):
        return []
    out = [m for m in DIP_MILESTONES_BASE if ratio >= m]
    m = 2000
    while m <= ratio and m <= DIP_MAX_X:
        out.append(m)
        m += 500
    return out


def _dip_sent_levels(call):
    v = call.get("sent_dip_milestones")
    if not isinstance(v, list):
        v = []
        call["sent_dip_milestones"] = v
    return v


def call_peak_ratio(call):
    """Best known ATH multiplier for a tracked call (never below live ratio)."""
    try:
        pr = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        pr = 0.0
    try:
        lr = float(call.get("last_ratio", 0) or 0)
    except (TypeError, ValueError):
        lr = 0.0
    return max(pr, lr)


def _update_peak(call, ratio, cur_mc=0.0):
    """Store a new all-time-high ratio/MC on the call. Returns True if raised."""
    try:
        ratio = float(ratio or 0)
    except (TypeError, ValueError):
        return False
    if ratio <= 0:
        return False
    prev = 0.0
    try:
        prev = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        prev = 0.0
    if ratio <= prev + 1e-9:
        return False
    entry_mc = 0.0
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
    except (TypeError, ValueError):
        entry_mc = 0.0
    peak_mc = cur_mc if cur_mc and cur_mc > 0 else (entry_mc * ratio if entry_mc > 0 else 0.0)
    call["peak_ratio"] = round(ratio, 4)
    if peak_mc > 0:
        call["peak_mc"] = peak_mc
        call["peak_mc_fmt"] = fmt_mc(peak_mc)
    call["peak_at"] = datetime.utcnow().isoformat()
    return True


def _verified_live_ratio_inner(call, dex):
    """Return a market-cap-first live ratio, or 0 when the sample is unsafe.

    Price ratios are not interchangeable across pool migrations, decimal/supply
    changes or bad pair responses. Market cap is therefore authoritative. A
    price ratio may only be used when MC is unavailable, and conflicting price
    data can never inflate a valid MC ratio - UNLESS the MC field itself looks
    stale/frozen (see below), in which case it's not really "data" anymore.
    """
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
        entry_price = float(call.get("entry_price", 0) or 0)
        cur_mc = float((dex or {}).get("mcap", 0) or 0)
        cur_price = float((dex or {}).get("price", 0) or 0)
    except (TypeError, ValueError):
        return 0.0

    # OWNER FIX (2026-09-19, PDOG + repeat incidents): GeckoTerminal /
    # DexPaprika / Birdeye fall back to FDV (fully-diluted valuation) when a
    # token has no real circulating-supply market_cap field. FDV counts
    # locked/team/treasury supply too, so it can read 3-10x+ higher than the
    # actual circulating MC shown on-chart - a single FDV-tagged tick was
    # enough to fire fake 4X/30X/40X alerts on tokens that hadn't moved.
    # This was previously only guarded at entry-MC time (_reconcile_entry_mc,
    # live_is_fdv=...); ongoing live-ratio checks had no such guard. Since
    # every X-alert/milestone/record path ultimately calls this one function
    # for its ratio, checking `_mc_is_fdv` here protects all of them at once
    # - an FDV-derived MC is treated as "no MC data this tick" (falls back to
    # price-ratio, or 0.0 / no alert) instead of ever being trusted directly.
    if (dex or {}).get("_mc_is_fdv"):
        cur_mc = 0.0

    mc_ratio = cur_mc / entry_mc if entry_mc > 0 and cur_mc > 0 else 0.0
    price_ratio = cur_price / entry_price if entry_price > 0 and cur_price > 0 else 0.0
    if mc_ratio > 0:
        if price_ratio > 0:
            # FIX (rug-pull supply-manipulation bug, SCMP/@robinhoodcallsbot
            # incident, 2026-09-20): price_ratio and mc_ratio come from the
            # SAME DexScreener response but can be computed from different
            # fields (raw price vs the reported marketCap/totalSupply). A
            # legitimate supply change (burn/mint) can only make mc_ratio
            # diverge from price_ratio in magnitude - it can NEVER flip which
            # DIRECTION the token moved, because MC is mathematically
            # price × supply: if price is really down, MC cannot be really up
            # unless supply itself exploded, which is exactly what a rug
            # contract does to its own reported supply. Here price_ratio was
            # 0.61x (correctly showing the token had crashed - real MC ~2K
            # against an entry of 4.8K) while mc_ratio claimed 39.73x - the
            # old code trusted mc_ratio purely because it wasn't "frozen at
            # exactly 1.00x", and fired 16X through 35X fake milestone alerts
            # on a dead token. A direction conflict (one side up, one side
            # down) is a much stronger and simpler red flag than the narrow
            # frozen-at-1.0 case below, and price is far harder to fake than a
            # reported supply/MC field, so it wins outright whenever they
            # disagree on direction - no divergence threshold needed.
            if (price_ratio < 0.97 and mc_ratio > 1.03) or (price_ratio > 1.03 and mc_ratio < 0.97):
                logger.warning(
                    f"🚫 Direction conflict: price ratio {price_ratio:.2f}x vs "
                    f"DexScreener MC ratio {mc_ratio:.2f}x for "
                    f"{call.get('ca','')[:12]}… - MC/supply field is almost "
                    f"certainly manipulated or stale (likely a rug); trusting "
                    f"price, which cannot be faked the same way.")
                return price_ratio
            divergence = max(mc_ratio, price_ratio) / max(min(mc_ratio, price_ratio), 1e-12)
            if divergence > 3:
                # OWNER FIX (2026-09-14): DexScreener's cached `marketCap`
                # field can freeze for hours at a time while `priceUsd` keeps
                # updating normally - a stale field on their side, not the
                # token actually standing still. This is only reliably
                # detectable in one specific case: MC frozen almost exactly
                # AT the entry value (mc_ratio ~1.00x) for a call that has
                # clearly moved - a real, actively-traded token essentially
                # never sits at exactly 1.000x for many ticks in a row, so
                # that coincidence is a strong, narrow signal the field
                # itself is stuck (proven correct for the DOGGO/minibangcall
                # incident: entry pinned every X at 1x forever).
                #
                # REVERTED (2026-09-14, same day, IDX/casinoinsiderhere
                # incident): a broader "MC unchanged for N ticks at ANY
                # level → treat as stale" version briefly replaced this and
                # made things worse - it fired on this token's mc_ratio
                # sitting normally at ~1.33-1.41x (a real, legitimately flat
                # stretch, not stale data) while price_ratio bounced around
                # 0.28-0.30x from ordinary quote noise, and confidently
                # trusted the WRONG noisy price over the CORRECT flat MC.
                # A token's market cap simply not moving much for a while is
                # completely normal and is not by itself evidence of a stuck
                # field - only the exact-1.00x coincidence is. Narrowed back.
                mc_frozen_at_entry = abs(mc_ratio - 1.0) < 0.03
                if mc_frozen_at_entry:
                    # BUG FIX (2026-09-14, live incident - channel called a ~1T MC
                    # token, entry got captured in the "millions" instead, then the
                    # bot spammed 2X/3X/4X/... alerts back to back): trusting price
                    # unconditionally whenever MC looks frozen was too permissive.
                    # A real pump ratchets up gradually tick by tick; a jump straight
                    # from ~1x to thousands-of-x in one sample (5575x was observed
                    # live) is virtually always bad data - wrong decimals, a
                    # rebased/forked pair, or a bad quote - not a real move. Trusting
                    # it meant every milestone from 2X up to 1000X+ fired at once in
                    # a single tick: an alert-spam burst, not thousands of real X.
                    # Cap it: an implausible jump is rejected and held at the call's
                    # last confirmed peak instead of being allowed through.
                    _prev_peak = 0.0
                    try:
                        _prev_peak = float(call.get("peak_ratio", 0) or 0)
                    except (TypeError, ValueError):
                        _prev_peak = 0.0
                    _implausible = price_ratio > 1000 or (
                        _prev_peak > 50 and price_ratio > _prev_peak * 50)
                    if _implausible:
                        logger.warning(
                            f"Rejecting implausible price ratio {price_ratio:.2f}x "
                            f"(prev peak {_prev_peak:.2f}x, MC ratio {mc_ratio:.2f}x) "
                            f"for {call.get('ca','')[:12]}… - likely bad decimals/pair "
                            f"data, not a real move; holding at last-known ratio")
                        return _prev_peak if _prev_peak > 0 else mc_ratio
                    logger.warning(
                        f"MC ratio frozen at entry ({mc_ratio:.2f}x) but price ratio is "
                        f"{price_ratio:.2f}x for {call.get('ca','')[:12]}… - trusting price instead")
                    return price_ratio
                # FIX (2026-09-30, BABYBUN/@spawnscalls fake 30X): MC = price x
                # supply, so an MC ratio that claims a big pump (>=2x) while
                # PRICE says it moved >3x less is the supply/MC field being
                # wrong or stale, not a real 26x. A pump claim must be backed by
                # price too - take the lower of the two. (Ratios < 2x are left
                # alone so the IDX/DOGGO fixes above keep behaving as before.)
                if MC_PRICE_PUMP_GUARD and mc_ratio >= 2 and mc_ratio > price_ratio:
                    logger.warning(
                        f"🛡️ MC-vs-price pump guard: MC ratio {mc_ratio:.2f}x but price "
                        f"ratio only {price_ratio:.2f}x for {call.get('ca','')[:12]}… "
                        f"- using price ratio")
                    return price_ratio
                logger.warning(
                    f"Ignoring conflicting price ratio {price_ratio:.2f}x; "
                    f"DexScreener MC ratio is {mc_ratio:.2f}x for {call.get('ca','')[:12]}…")
        return mc_ratio
    # Last-resort price tracking is deliberately conservative. Once a real MC
    # appears, all future ticks automatically switch to MC-first tracking.
    if 0 < price_ratio <= 1000:
        return price_ratio
    return 0.0


def _verified_live_ratio(call, dex):
    """Outer safety wrapper around _verified_live_ratio_inner().

    Every specific bad-data pattern found so far (frozen-at-1.0x MC field,
    price/MC direction conflicts, FDV-tagged MC, implausible one-tick jumps
    while frozen) has its own targeted fix inside the inner function. Each
    one only catches the exact pattern of the incident that revealed it -
    which is why this exact class of bug (a rugged/dead token being handed a
    wildly wrong MC from SOME source, in SOME shape nobody has hit yet) kept
    recurring under new disguises. This wrapper is the backstop: regardless
    of which internal path produced a number, or whether it's a shape this
    file has ever seen before, a jump this large in a single tick relative to
    the call's own last CONFIRMED peak is rejected outright and the peak is
    held instead - a real pump does not advance 20x+ beyond its own prior
    high between two ~2-second monitoring ticks. This does not replace the
    inner checks (they still produce a better-reasoned number/log message for
    the common cases); it only adds an absolute ceiling nothing can slip
    past, including patterns this file doesn't have a name for yet.
    """
    ratio = _verified_live_ratio_inner(call, dex)
    if ratio <= 0:
        return ratio
    # FIX (2026-10-18, $DACITY fake 2X..13X burst after a redeploy): a quote that came from an
    # ALTERNATE source (GeckoTerminal/Birdeye/...) -- e.g. while DexScreener is rate-limited or
    # right after a restart -- can be frozen/stale (Gecko kept returning 44.4K all day while
    # DexScreener showed 3.3K). The old code let such a quote produce ANY ratio up to 20x/1000x
    # when the call had no established peak yet. An alt-source quote may now never imply more
    # than the same ceiling the "unstick" probe uses (max(8x, +5x) over the last peak);
    # DexScreener's own quotes are unaffected.
    _qsrc = str((dex or {}).get("_source") or "").lower()
    if _qsrc and _qsrc != "dexscreener" and ALT_SOURCE_STRICT:
        # FIX (2026-10-21, "200k peak -> 40k par call -> free X"): token pehle 200k tak gaya, 40k par aa gaya,
        # caller ne 40k par call ki. Flat (~1.0x) tokens par alt-source probe chalta hai; Gecko/Birdeye
        # ka purana 200k (stale peak) 5x bana kar nikalta tha aur purana 8x ceiling usay pass kar deta tha.
        # Ab alt source akele X nahi bana sakta: ratio <= max(ALT_FREE_RATIO, DexScreener ratio * ALT_MAX_LEAD).
        # DexScreener ka apna reading na ho (ya purana ho) to sirf ALT_FREE_RATIO (1.9x, 2X se neeche) tak.
        try:
            _dx_r = float(call.get("_dx_ratio", 0) or 0)
            _dx_t = float(call.get("_dx_at", 0) or 0)
        except (TypeError, ValueError):
            _dx_r, _dx_t = 0.0, 0.0
        _fresh_dx = _dx_r > 0 and (time.time() - _dx_t) <= ALT_DX_MAX_AGE_S
        _alt_ceiling = max(ALT_FREE_RATIO, _dx_r * ALT_MAX_LEAD) if _fresh_dx else ALT_FREE_RATIO
        if ratio > _alt_ceiling:
            logger.warning(
                f"🚫 Alt-source ratio {ratio:.2f}x ({_qsrc}) rejected for {call.get('ca','')[:12]}… - "
                f"DexScreener-confirmed ratio {_dx_r:.2f}x (alt ceiling {_alt_ceiling:.2f}x). "
                f"Alt source akele X nahi bana sakta (stale-peak guard).")
            return min(call_peak_ratio(call), _alt_ceiling) if call_peak_ratio(call) > 0 else 0.0
    if _qsrc and _qsrc != "dexscreener":
        _base = max(call_peak_ratio(call), 1.0)
        if ratio > max(_base * 8, _base + 5):
            logger.warning(
                f"🚫 Rejecting alt-source ratio {ratio:.2f}x ({_qsrc}) for "
                f"{call.get('ca','')[:12]}… - too far above last peak {_base:.2f}x for an "
                f"unverified source; waiting for DexScreener to confirm.")
            return call_peak_ratio(call)
    prev_peak = call_peak_ratio(call)
    if prev_peak <= 0:
        # First-ever real reading for this call - no established baseline to
        # compare against yet, so only guard against obviously-broken data
        # (wrong decimals etc.), same ceiling already used for that case.
        if ratio > 1000:
            logger.warning(
                f"🚫 Rejecting implausible first-ever ratio {ratio:.2f}x for "
                f"{call.get('ca','')[:12]}… (no established peak yet) - "
                f"holding at 0 (no alert this tick).")
            return 0.0
        return ratio
    if ratio > max(prev_peak * 20, 50):
        logger.warning(
            f"🚫 Rejecting implausible jump {prev_peak:.2f}x → {ratio:.2f}x "
            f"for {call.get('ca','')[:12]}… (>20x prior peak in one tick) - "
            f"holding at last confirmed peak {prev_peak:.2f}x instead.")
        return prev_peak
    return ratio


_DEX_BULK_CURSOR = 0


_SEL_STATE = {"tick": {"bulk": 0, "prio": 0}, "ucur": {"bulk": 0, "prio": 0}, "log_at": {"bulk": 0.0, "prio": 0.0}}


_FB_NEXT_AT: dict = {}          # ca -> next time single-CA fallback allowed


HOT_WINDOW_SECONDS      = int(os.environ.get("HOT_WINDOW_SECONDS", "180") or 180)


WARM_EVERY_N_TICKS      = max(1, int(os.environ.get("WARM_EVERY_N_TICKS", "2") or 2))


REGULAR_MIN_SHARE       = min(0.9, max(0.1, float(os.environ.get("REGULAR_MIN_SHARE", "0.4") or 0.4)))


GLITCH_BACKOFF_BASE_S   = float(os.environ.get("GLITCH_BACKOFF_BASE_SECONDS", "3") or 3)


GLITCH_BACKOFF_MAX_S    = float(os.environ.get("GLITCH_BACKOFF_MAX_SECONDS", "20") or 20)


REG_YOUNG_S      = float(os.environ.get("REG_YOUNG_S", "20") or 20)      # age <= 6h


REG_MID_S        = float(os.environ.get("REG_MID_S", "90") or 90)        # 6h - 48h


REG_OLD_S        = float(os.environ.get("REG_OLD_S", "300") or 300)      # > 48h


REG_DEAD_S       = float(os.environ.get("REG_DEAD_S", "900") or 900)     # liq & vol dono bohot kam


REG_ACTIVE_S     = float(os.environ.get("REG_ACTIVE_S", "8") or 8)


REG_DEAD_LIQ     = float(os.environ.get("REG_DEAD_LIQ", "300") or 300)


REG_DEAD_VOL     = float(os.environ.get("REG_DEAD_VOL", "500") or 500)


REG_ACTIVE_RATIO = float(os.environ.get("REG_ACTIVE_RATIO", "1.4") or 1.4)


REG_ACTIVE_PC1   = float(os.environ.get("REG_ACTIVE_PC1", "25") or 25)


_NEXT_CHK: dict = {}            # call_key -> next due time (memory only)


def _mon_interval(c, age):
    """Seconds until this (non-urgent) call should be checked again."""
    try:
        lr = float(c.get("last_ratio", 0) or 0)
    except (TypeError, ValueError):
        lr = 0.0
    try:
        pc1 = abs(float(c.get("_pc1", 0) or 0))
    except (TypeError, ValueError):
        pc1 = 0.0
    if lr >= REG_ACTIVE_RATIO or pc1 >= REG_ACTIVE_PC1:
        return REG_ACTIVE_S if age <= 86400 else max(REG_ACTIVE_S, 20.0)
    if "liquidity_usd" in c:
        try:
            liq = float(c.get("liquidity_usd", 0) or 0)
            vol = float(c.get("volume_h24", 0) or 0)
        except (TypeError, ValueError):
            liq = vol = 1e9
        if age > 1800 and liq < REG_DEAD_LIQ and vol < REG_DEAD_VOL:
            return REG_DEAD_S
    if age <= 6 * 3600:
        return REG_YOUNG_S
    if age <= 48 * 3600:
        return REG_MID_S
    return REG_OLD_S


ALT_SOURCE_STRICT       = (os.environ.get("ALT_SOURCE_STRICT", "1") or "1").strip().lower() in ("1", "true", "yes")


ALT_FREE_RATIO          = float(os.environ.get("ALT_FREE_RATIO", "1.9") or 1.9)       # is se upar sirf DexScreener-confirmed


ALT_MAX_LEAD            = float(os.environ.get("ALT_MAX_LEAD", "1.25") or 1.25)       # alt <= DexScreener ratio * 1.25


ALT_DX_MAX_AGE_S        = float(os.environ.get("ALT_DX_MAX_AGE_SECONDS", "900") or 900)


MONITOR_TICK_TIMEOUT_SECONDS = int(os.environ.get("MONITOR_TICK_TIMEOUT_SECONDS", "20") or 20)

# === MONBOT STATE ============================================================
# _calls: private deep copies of the main bot's tracked calls (read-only
# source, reloaded when tracked_calls.json mtime changes).
# _sent: {call_key: set(ms)} sealed milestones from sent_milestones.json.
# _peaks: {call_key: {...}} persisted peak/spike state (monbot_peaks.json).
# _dip: {call_key: {...}} persisted dip-low state (monbot_dip.json).
# _queue: {queue_key: entry} pending milestones (monbot_milestones.json).
# _queued_set: {(call_key, ms, type)} fast lookup for already-queued levels.
_calls = {}
_sent = {}
_peaks = {}
_dip = {}
_queue = {}
_queued_set = set()
_mtimes = {"tracked": 0.0, "sent": 0.0}
_peaks_dirty = False
_peaks_last_save = 0.0
_dip_dirty = False
_dip_last_save = 0.0
_BULK_TICK = 0
_monitor_alt_probe_at: dict = {}  # ca -> next time an alt-source probe is allowed
_QUEUE_MAX_AGE_S = 24 * 3600


def _queue_key(call_key, x_val, typ):
    return f"{call_key}:{x_val}:{typ}"


def _age_seconds(call):
    try:
        return (datetime.utcnow() - datetime.fromisoformat(
            str(call.get("tracked_since", "")))).total_seconds()
    except Exception:
        return 999999.0


# --- peaks state ---
def _seed_peaks(call_key, call):
    """First sight of a call: seed peak state from the tracked record."""
    global _peaks_dirty
    if call_key in _peaks:
        return
    try:
        pr = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        pr = 0.0
    try:
        bx = float(call.get("best_x", 0) or 0)
    except (TypeError, ValueError):
        bx = 0.0
    ratio = max(pr, bx)
    try:
        pmc = float(call.get("peak_mc", 0) or 0)
    except (TypeError, ValueError):
        pmc = 0.0
    if pmc <= 0 and ratio > 0:
        try:
            pmc = float(call.get("entry_mc", 0) or 0) * ratio
        except (TypeError, ValueError):
            pmc = 0.0
    _peaks[call_key] = {
        "peak_ratio": ratio,
        "peak_mc": pmc,
        "last_ratio": ratio,
        "last_confirmed_ratio": ratio,
        "spike_pending_ratio": 0.0,
        "spike_pending_at": "",
        "glitch_until": 0.0,
        "glitch_n": 0,
        "dx_ratio": 0.0,
        "dx_at": 0.0,
        "pc1": 0.0,
    }
    _peaks_dirty = True


def _apply_peaks_to_call(call_key, call):
    """Re-apply persisted monbot state onto a freshly reloaded call copy."""
    st = _peaks.get(call_key)
    if not st:
        return
    try:
        if float(st.get("peak_ratio", 0) or 0) > 0:
            call["peak_ratio"] = float(st["peak_ratio"])
            call["peak_mc"] = float(st.get("peak_mc", 0) or 0)
    except (TypeError, ValueError):
        pass
    call["_last_confirmed_ratio"] = float(st.get("last_confirmed_ratio", 0) or 0)
    call["last_ratio"] = float(st.get("last_ratio", 0) or 0)
    if float(st.get("spike_pending_ratio", 0) or 0) > 0:
        call["_spike_pending_ratio"] = float(st["spike_pending_ratio"])
        call["_spike_pending_at"] = st.get("spike_pending_at", "")
    if float(st.get("glitch_until", 0) or 0) > 0:
        call["_glitch_until"] = float(st["glitch_until"])
        call["_glitch_n"] = int(st.get("glitch_n", 0) or 0)
    if float(st.get("dx_ratio", 0) or 0) > 0:
        call["_dx_ratio"] = float(st["dx_ratio"])
        call["_dx_at"] = float(st.get("dx_at", 0) or 0)
    call["_pc1"] = float(st.get("pc1", 0) or 0)


def _sync_call_to_peaks(call_key, call):
    """Copy peak/spike state from a private call copy into _peaks."""
    global _peaks_dirty
    st = _peaks.setdefault(call_key, {})
    changed = False
    try:
        pr = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        pr = 0.0
    try:
        pmc = float(call.get("peak_mc", 0) or 0)
    except (TypeError, ValueError):
        pmc = 0.0
    if pr > float(st.get("peak_ratio", 0) or 0):
        st["peak_ratio"] = pr
        st["peak_mc"] = pmc
        changed = True  # new peak: persist promptly
    for field, call_field in (
            ("last_ratio", "last_ratio"),
            ("last_confirmed_ratio", "_last_confirmed_ratio"),
            ("spike_pending_ratio", "_spike_pending_ratio"),
            ("glitch_until", "_glitch_until"),
            ("glitch_n", "_glitch_n"),
            ("dx_ratio", "_dx_ratio"),
            ("dx_at", "_dx_at"),
            ("pc1", "_pc1")):
        v = call.get(call_field, 0)
        try:
            v = float(v or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v != float(st.get(field, 0) or 0):
            st[field] = v
            changed = True
    spa = call.get("_spike_pending_at", "")
    if spa != st.get("spike_pending_at", ""):
        st["spike_pending_at"] = spa
        changed = True
    if changed:
        _peaks_dirty = True
    return changed


def _maybe_save_peaks(force=False):
    global _peaks_dirty, _peaks_last_save
    now = time.time()
    if not _peaks_dirty:
        return
    if not force and now - _peaks_last_save < 60:
        return
    try:
        _atomic_write_json(MONBOT_PEAKS_FILE, _peaks)
        _peaks_last_save = now
        _peaks_dirty = False
    except Exception as e:
        logger.warning(f"monbot: peaks save failed: {e}")


# --- dip state ---
def _apply_dip_to_call(call_key, call):
    st = _dip.get(call_key)
    if not st:
        return
    for field in ("dip_low", "_dip_low_candidate", "_dip_low_candidate_ticks",
                  "dip_low_at_alert", "sent_dip_milestones", "pending_dip"):
        if field in st:
            call[field] = copy.deepcopy(st[field])


def _sync_call_to_dip(call_key, call):
    global _dip_dirty
    st = _dip.setdefault(call_key, {})
    changed = False
    for field in ("dip_low", "_dip_low_candidate", "_dip_low_candidate_ticks",
                  "dip_low_at_alert", "sent_dip_milestones", "pending_dip"):
        v = copy.deepcopy(call.get(field))
        if v != st.get(field):
            st[field] = v
            changed = True
    if changed:
        _dip_dirty = True
    return changed


def _maybe_save_dip(force=False):
    global _dip_dirty, _dip_last_save
    now = time.time()
    if not _dip_dirty:
        return
    if not force and now - _dip_last_save < 60:
        return
    try:
        _atomic_write_json(MONBOT_DIP_FILE, _dip)
        _dip_last_save = now
        _dip_dirty = False
    except Exception as e:
        logger.warning(f"monbot: dip save failed: {e}")


# --- queue ---
def _queue_entry(call_key, call, x_val, typ, peak_ratio, live_ratio, cur_mc,
                 ms_mc_fmt, record_only=False, dip_low=0.0):
    key = _queue_key(call_key, x_val, typ)
    if key in _queue:
        return False
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
    except (TypeError, ValueError):
        entry_mc = 0.0
    entry = {
        "type": typ,
        "call_key": call_key,
        "channel": call.get("channel", ""),
        "ca": call.get("ca", ""),
        "symbol": call.get("symbol", ""),
        "x_val": int(x_val),
        "peak_mc": float(call.get("peak_mc", 0) or 0),
        "ms_mc_fmt": ms_mc_fmt,
        "entry_mc": entry_mc,
        "entry_fmt": call.get("entry_fmt", "N/A"),
        "live_ratio": round(float(live_ratio or 0), 4),
        "detected_at": datetime.utcnow().isoformat(),
        "status": "queued",
        "record_only": bool(record_only),
    }
    if typ == "dip":
        entry["dip_low"] = float(dip_low or 0)
        entry["cur_mc"] = float(cur_mc or 0)
    _queue[key] = entry
    _queued_set.add((call_key, int(x_val), typ))
    try:
        _atomic_write_json(MONBOT_QUEUE_FILE, _queue)
    except Exception as e:
        logger.warning(f"monbot: queue save failed: {e}")
    logger.info(
        f"monbot: queued {x_val}X {call.get('symbol', '?')} "
        f"@{call.get('channel', '?')} peak={peak_ratio:.2f} live={live_ratio:.2f}")
    return True


def _prune_queue():
    """Drop queued entries that are sealed, orphaned, or older than 24h."""
    global _queue, _queued_set
    now = datetime.utcnow()
    drop = []
    for key, e in list(_queue.items()):
        ck = e.get("call_key", "")
        typ = e.get("type", "x")
        xv = e.get("x_val", 0)
        try:
            age = (now - datetime.fromisoformat(str(e.get("detected_at", "")))).total_seconds()
        except Exception:
            age = 0.0
        if age > _QUEUE_MAX_AGE_S:
            drop.append(key)
            continue
        if ck not in _calls:
            drop.append(key)
            continue
        if typ == "x":
            if xv in _sent.get(ck, set()):
                drop.append(key)
        else:
            call = _calls.get(ck) or {}
            sent_dip = call.get("sent_dip_milestones") or []
            try:
                if int(xv) in [int(x) for x in sent_dip]:
                    drop.append(key)
            except (TypeError, ValueError):
                pass
    if not drop:
        return
    for key in drop:
        e = _queue.pop(key, None)
        if e:
            _queued_set.discard((e.get("call_key", ""), int(e.get("x_val", 0)), e.get("type", "x")))
    try:
        _atomic_write_json(MONBOT_QUEUE_FILE, _queue)
    except Exception as ex:
        logger.warning(f"monbot: queue prune save failed: {ex}")
    logger.info(f"monbot: pruned {len(drop)} queued entries")


# --- data reload (READ-ONLY source, reload when mtime changes) ---
def _reload_main_data():
    global _calls, _sent
    changed = False
    try:
        mt = os.path.getmtime(MAIN_TRACKED_FILE)
    except OSError:
        mt = 0.0
    try:
        ms = os.path.getmtime(MAIN_MILESTONES_FILE)
    except OSError:
        ms = 0.0
    if mt != _mtimes["tracked"]:
        raw = _read_json_file(MAIN_TRACKED_FILE, {}) or {}
        if isinstance(raw, dict) and raw:
            new_calls = {}
            for k, v in raw.items():
                if not isinstance(v, dict):
                    continue
                c = copy.deepcopy(v)
                _seed_peaks(k, c)
                _apply_peaks_to_call(k, c)
                _apply_dip_to_call(k, c)
                new_calls[k] = c
            _calls = new_calls
            _mtimes["tracked"] = mt
            changed = True
    if ms != _mtimes["sent"]:
        raw = _read_json_file(MAIN_MILESTONES_FILE, {}) or {}
        new_sent = {}
        if isinstance(raw, dict):
            for k, v in raw.items():
                try:
                    new_sent[k] = {int(x) for x in (v or [])}
                except (TypeError, ValueError):
                    new_sent[k] = set()
        _sent = new_sent
        _mtimes["sent"] = ms
        changed = True
    return changed


def _load_own_files():
    global _peaks, _dip, _queue, _queued_set
    _peaks = _read_json_file(MONBOT_PEAKS_FILE, {}) or {}
    _dip = _read_json_file(MONBOT_DIP_FILE, {}) or {}
    _queue = _read_json_file(MONBOT_QUEUE_FILE, {}) or {}
    _queued_set = set()
    for key, e in _queue.items():
        try:
            _queued_set.add((e.get("call_key", ""), int(e.get("x_val", 0)), e.get("type", "x")))
        except (TypeError, ValueError):
            pass


# === DIP DETECTION (adapted from _dip_check_tick, x_alert.py) ===============
# Same low-tracking rules as the main bot. Difference: instead of calling
# send_dip_alert (which posts), due dip milestones are QUEUED in
# monbot_milestones.json for the main bot's monbot_import_job to post.
async def _monbot_dip_tick(call_key, call, cur_mc):
    if not dip_enabled():
        return
    try:
        entry_mc = float(call.get("entry_mc") or 0)
        cur_mc = float(cur_mc or 0)
    except (TypeError, ValueError):
        return
    if entry_mc <= 0 or cur_mc <= 0:
        return
    threshold = dip_threshold_pct()
    dip_low = call.get("dip_low")
    try:
        dip_low = float(dip_low) if dip_low else 0.0
    except (TypeError, ValueError):
        dip_low = 0.0
    if cur_mc <= entry_mc * (1.0 - threshold / 100.0):
        if cur_mc >= entry_mc * DIP_DUST_FLOOR_FRAC:
            cand = call.get("_dip_low_candidate")
            try:
                cand = float(cand) if cand else 0.0
            except (TypeError, ValueError):
                cand = 0.0
            if cand <= 0 or cur_mc < cand:
                call["_dip_low_candidate"] = cur_mc
                call["_dip_low_candidate_ticks"] = 1
            elif cur_mc <= cand * 1.05:
                call["_dip_low_candidate_ticks"] = int(call.get("_dip_low_candidate_ticks") or 0) + 1
            else:
                call["_dip_low_candidate"] = cur_mc
                call["_dip_low_candidate_ticks"] = 1
            if int(call.get("_dip_low_candidate_ticks") or 0) >= DIP_LOW_CONFIRM_TICKS:
                cand = float(call.get("_dip_low_candidate") or 0)
                if cand > 0:
                    dip_low = cand if dip_low <= 0 else min(dip_low, cand)
                    call["dip_low"] = dip_low
                    try:
                        cycle_low = float(call.get("dip_low_at_alert") or 0)
                    except (TypeError, ValueError):
                        cycle_low = 0.0
                    if cycle_low <= 0:
                        call["dip_low_at_alert"] = cand
                    elif cand <= cycle_low * DIP_RESET_BELOW_FRAC:
                        call["sent_dip_milestones"] = []
                        call["pending_dip"] = []
                        call.pop("dip_pending_mc", None)
                        call["dip_low_at_alert"] = cand
                        logger.info(f"Dip cycle reset {call_key}: new low "
                                    f"{fmt_mc(cand)} is 20%+ below cycle low "
                                    f"{fmt_mc(cycle_low)} - sent dip milestones cleared")
                call.pop("_dip_low_candidate", None)
                call.pop("_dip_low_candidate_ticks", None)
    if dip_low <= 0:
        _sync_call_to_dip(call_key, call)
        return
    dip_ratio = cur_mc / dip_low
    if dip_ratio < 2:
        _sync_call_to_dip(call_key, call)
        return
    sent = _dip_sent_levels(call)
    # Never re-queue a level the main bot already sealed in its own record.
    main_sent = call.get("sent_dip_milestones") or []
    due = [m for m in dip_milestones_for_ratio(dip_ratio)
           if m not in sent and m not in main_sent]
    if not due:
        _sync_call_to_dip(call_key, call)
        return
    due.sort()
    for ms in due:
        if (call_key, int(ms), "dip") in _queued_set:
            continue
        ms_mc_fmt = fmt_mc(entry_mc * ms) if entry_mc > 0 else fmt_mc(cur_mc)
        if _queue_entry(call_key, call, ms, "dip", dip_ratio, dip_ratio,
                        cur_mc, ms_mc_fmt, dip_low=dip_low):
            sent.append(int(ms))
    _sync_call_to_dip(call_key, call)


# === MONBOT CHECK_ONE (detection only) =======================================
# Mirrors the main bot's check_one (x_alert.py _monitoring_job_body) with
# these changes: never writes main bot files, all state goes to monbot's own
# in-memory dicts + files, no entry fill / confirm task / early-rebase /
# point deduction / hot-streak / series writes, GUARD rule on queueing.
async def monbot_check_one(call_key, call, ctx):
    try:
        if call.get("frozen"):
            return
        dex = _get_cached_dex(call["ca"])
        if dex is None:
            if not (call.get("alert_pending") or call.get("dex_pending")
                    or _age_seconds(call) <= 300):
                return
            if ctx["fb"]["used"] >= ctx["fb"]["max"]:
                return
            fb_ca = call["ca"]
            fb_now = time.time()
            if fb_now < _FB_NEXT_AT.get(fb_ca, 0):
                return
            fb_age = _age_seconds(call)
            _FB_NEXT_AT[fb_ca] = fb_now + (0.0 if fb_age <= 45 else 2.0 if fb_age <= 180
                                           else 6.0 if fb_age <= 600 else 20.0)
            if len(_FB_NEXT_AT) > 6000:
                for k_ in [k for k, v in list(_FB_NEXT_AT.items()) if v < fb_now][:3000]:
                    _FB_NEXT_AT.pop(k_, None)
            ctx["fb"]["used"] += 1
            async with ctx["sem"]:
                lane_tok = _DEX_LANE_CV.set("mon")
                try:
                    dex = await asyncio.wait_for(
                        fetch_dexscreener(call["ca"], allow_alt=False, retries=1, min_liquidity=0),
                        timeout=ctx["fb_timeout"])
                except asyncio.TimeoutError:
                    logger.warning(f"Market-data primary timeout: {call['ca'][:12]}...")
                    dex = None
                finally:
                    try:
                        _DEX_LANE_CV.reset(lane_tok)
                    except Exception:
                        pass
        if dex is None:
            return

        try:
            dex_ratio = _verified_live_ratio(call, dex)
        except Exception:
            dex_ratio = 0.0
        try:
            if dex_ratio > 0 and str((dex or {}).get("_source") or "dexscreener").lower() == "dexscreener":
                call["_dx_ratio"] = round(float(dex_ratio), 4)
                call["_dx_at"] = time.time()
                call["_pc1"] = round(float((dex or {}).get("price_change_h1", 0) or 0), 1)
        except Exception:
            pass

        if (EXTRA_SOURCES_ENABLED and (dex_ratio == 0.0 or 0.90 <= dex_ratio < 1.10)):
            ca_probe = call.get("ca", "")
            now_probe = time.time()
            if now_probe >= _monitor_alt_probe_at.get(ca_probe, 0):
                _monitor_alt_probe_at[ca_probe] = now_probe + 4.0
                try:
                    async with ctx["sem"]:
                        if ctx["fb"]["used"] >= ctx["fb"]["max"]:
                            alt_quote = None
                        else:
                            ctx["fb"]["used"] += 1
                            alt_quote = await asyncio.wait_for(
                                asyncio.to_thread(_fetch_alt_sources_sync, ca_probe),
                                timeout=ctx["alt_timeout"])
                except Exception:
                    alt_quote = None
                if alt_quote:
                    alt_ratio = _verified_live_ratio(call, alt_quote)
                    plausible_jump = alt_ratio <= max(dex_ratio * 8, dex_ratio + 5)
                    if alt_ratio > max(dex_ratio * 1.15, dex_ratio + 0.10) and plausible_jump:
                        logger.info(
                            f"fresher alternate quote selected "
                            f"{call.get('symbol', '?')} "
                            f"{dex_ratio:.2f}X -> {alt_ratio:.2f}X "
                            f"({alt_quote.get('_source', 'alt')})")
                        dex = alt_quote
                    elif alt_ratio > max(dex_ratio * 1.15, dex_ratio + 0.10):
                        logger.warning(
                            f"Ignoring implausible alt-source jump "
                            f"{call.get('symbol', '?')} {dex_ratio:.2f}X -> "
                            f"{alt_ratio:.2f}X ({alt_quote.get('_source', 'alt')}) "
                            f"- too large to be a genuine unstick, keeping DexScreener's read.")

        cur_price = dex.get("price", 0)
        cur_mc = dex.get("mcap", 0)
        if cur_mc > 1_000_000_000_000:
            logger.warning(f"Skipping unrealistic MC ${cur_mc:.0f} for {call['ca'][:12]}...")
            return

        # Symbol / chain fill on the PRIVATE copy only (no entry fill here:
        # entry correctness is owned by dropbot / main bot).
        if not call.get("symbol") and dex.get("symbol"):
            call["symbol"] = dex.get("symbol", "")
        if dex.get("chain"):
            call["chain"] = dex.get("chain", call.get("chain", "SOL"))

        # Rug / fake-MC gate.
        ok_live, rug_reason = _rug_status(call, dex)
        try:
            if rug_reason == "liq_glitch_retry":
                gn = int(call.get("_glitch_n", 0) or 0) + 1
                call["_glitch_n"] = gn
                call["_glitch_until"] = time.time() + min(
                    GLITCH_BACKOFF_MAX_S, GLITCH_BACKOFF_BASE_S * (2 ** min(gn - 1, 4)))
            elif call.get("_glitch_n") or call.get("_glitch_until"):
                call["_glitch_n"] = 0
                call["_glitch_until"] = 0
        except Exception:
            pass
        hard_rug = ("liquidity_pulled", "liquidity_pulled_pending",
                    "already_rugged", "absurd_mc", "bad_data", "no_mc")
        record_only = False
        if not ok_live:
            if rug_reason in hard_rug:
                return
            soft_silent = str(os.environ.get("SOFT_GATE_RECORD_ONLY", "0")).strip().lower() in ("1", "true", "yes")
            record_only = soft_silent
            logger.info(f"soft gate ({rug_reason}) {call.get('symbol', '?')} "
                        f"@{call.get('channel', '?')} - "
                        + ("record only, no channel post" if soft_silent
                           else "posting anyway (SOFT_GATE_RECORD_ONLY=0)"))

        entry_price = call.get("entry_price", 0)
        entry_mc = call.get("entry_mc", 0)
        ratio = _verified_live_ratio(call, dex)
        if ratio <= 0:
            return
        ratio = min(ratio, MAX_MILESTONE * 2)

        # Single-tick spike-glitch guard (state in monbot_peaks.json).
        prev_conf_ratio = float(call.get("_last_confirmed_ratio",
                                         call.get("last_ratio", 0)) or 0)
        spike_cap = max(prev_conf_ratio * 25, 50)
        if prev_conf_ratio > 0 and ratio > spike_cap:
            pend_r = float(call.get("_spike_pending_ratio", 0) or 0)
            if pend_r > 0 and 0.7 * pend_r <= ratio <= 1.3 * pend_r:
                call.pop("_spike_pending_ratio", None)
                call.pop("_spike_pending_at", None)
            else:
                logger.warning(
                    f"Spike-glitch guard: {call.get('symbol', '?')} "
                    f"@{call.get('channel', '?')} jumped {prev_conf_ratio:.2f}x -> "
                    f"{ratio:.2f}x in one tick - holding for confirmation")
                call["_spike_pending_ratio"] = ratio
                call["_spike_pending_at"] = datetime.utcnow().isoformat()
                ratio = prev_conf_ratio
                cur_mc = entry_mc * ratio if entry_mc > 0 else cur_mc
        else:
            call.pop("_spike_pending_ratio", None)
            call.pop("_spike_pending_at", None)
        call["_last_confirmed_ratio"] = ratio
        call["last_ratio"] = round(ratio, 4)
        call["liquidity_usd"] = float(dex.get("liquidity_usd", 0) or 0)
        call["volume_h24"] = float(dex.get("volume_h24", 0) or 0)
        call["txns_buys"] = int(dex.get("txns_buys", 0) or 0)
        call["txns_sells"] = int(dex.get("txns_sells", 0) or 0)

        # Real-time ATH on the private copy.
        _update_peak(call, ratio, cur_mc)
        peak_ratio = call_peak_ratio(call)
        _sync_call_to_peaks(call_key, call)

        # Same-tick plausibility cap (inline version of
        # _remove_impossible_milestones - no writes here).
        if entry_mc > 0:
            max_plausible_ratio = MAX_PLAUSIBLE_MC / entry_mc
            if peak_ratio > max_plausible_ratio:
                peak_ratio = max_plausible_ratio

        # Entry-confirmation gate: fire only if confirmed, or past the
        # fail-safe age (120s, 420s for RH). No confirm task runs here.
        if not call.get("entry_confirmed", True):
            gate_age = _age_seconds(call)
            gate_limit = 420 if str(call.get("chain") or "").upper() == "RH" else 120
            if gate_age <= gate_limit:
                return

        # Dip check (queues dip entries, never posts).
        try:
            await _monbot_dip_tick(call_key, call, cur_mc)
        except Exception as e_dip:
            logger.warning(f"dip check skipped {call_key}: {e_dip}")

        # Which milestones fire on this tick? Peak-based, as in the main bot.
        pending = [ms for ms in get_milestones()
                   if ms <= MAX_MILESTONE
                   and peak_ratio >= ms
                   and ms not in _sent.get(call_key, set())
                   and (call_key, ms, "x") not in _queued_set]
        if not pending:
            return
        pending.sort()

        # GUARD RULE: queue a level ONLY if the LIVE ratio this tick is at
        # or above it. Peak above + live below = do not queue this tick.
        # A token that dumped back below the level never fires a stale alert.
        for ms in pending:
            if ratio < ms:
                logger.info(
                    f"monbot: GUARD hold {ms}X {call.get('symbol', '?')} "
                    f"@{call.get('channel', '?')} peak={peak_ratio:.2f} live={ratio:.2f}")
                continue
            if ms == pending[-1] and cur_mc > 0 and cur_mc < 1_000_000_000_000:
                expected_mc = entry_mc * ms if entry_mc > 0 else 0
                if expected_mc > 0 and (cur_mc > expected_mc * 5 or cur_mc < expected_mc * 0.5):
                    ms_mc_fmt = fmt_mc(expected_mc)
                else:
                    ms_mc_fmt = fmt_mc(cur_mc)
            elif entry_mc > 0:
                ms_mc_fmt = fmt_mc(entry_mc * ms)
            elif cur_mc > 0:
                ms_mc_fmt = fmt_mc(cur_mc)
            else:
                ms_mc_fmt = dex.get("mcap_fmt", "N/A")
            _queue_entry(call_key, call, ms, "x", peak_ratio, ratio, cur_mc,
                         ms_mc_fmt, record_only=record_only)
    except Exception as e:
        logger.error(f"monitoring check failed {call_key}: {e}")
        return


# === MONBOT TICK (lane selection mirrors _monitoring_job_body) ===============
async def monbot_tick(prio_mode):
    global _BULK_TICK
    if not prio_mode:
        _BULK_TICK += 1
        if _BULK_TICK % 5 != 0:
            return
    try:
        _reload_main_data()
        items = list(_calls.items())
        if not items:
            return
        active_channels_lower = {c.lower() for c in load_channels()}
        items = [(k, v) for k, v in items if v.get("channel", "").lower() in active_channels_lower]
        items = [(k, v) for k, v in items if not v.get("rugged")]
        items = [(k, v) for k, v in items if not v.get("frozen")]
        prio = priority_channels_lower()
        if prio_mode:
            if not prio:
                return
            items = [kv for kv in items if kv[1].get("channel", "").lower() in prio]
            if not items:
                return
        elif prio:
            items = [kv for kv in items if kv[1].get("channel", "").lower() not in prio]

        now_utc = datetime.utcnow()
        stuck_cutoff = int(os.environ.get("STUCK_URGENT_CUTOFF_SECONDS", "3600") or 3600)
        urgent_window = int(os.environ.get("URGENT_WINDOW_SECONDS", "600") or 600)
        lane = "prio" if prio_mode else "bulk"
        _SEL_STATE["tick"][lane] += 1
        tick_n = _SEL_STATE["tick"][lane]
        now_ts = time.time()
        total_before = len(items)

        hot, warm, regular = [], [], []
        glitch_skipped = 0
        for kv in items:
            k, c = kv
            try:
                if float(c.get("_glitch_until", 0) or 0) > now_ts:
                    glitch_skipped += 1
                    continue
            except Exception:
                pass
            age = _age_seconds(c)
            pending_ok = bool((c.get("alert_pending") or c.get("dex_pending")) and age <= stuck_cutoff)
            if age <= HOT_WINDOW_SECONDS or (pending_ok and age <= 300):
                hot.append(kv)
            elif age <= urgent_window or pending_ok:
                warm.append(kv)
            else:
                regular.append(kv)
        hot.sort(key=lambda kv: _age_seconds(kv[1]))
        warm.sort(key=lambda kv: _age_seconds(kv[1]))
        urgent_all = hot + (warm if (tick_n % WARM_EVERY_N_TICKS == 0) else [])

        max_checks = max(20, int(os.environ.get(
            "MONITOR_PRIORITY_MAX_CHECKS_PER_TICK" if prio_mode else "MONITOR_MAX_CHECKS_PER_TICK",
            "130" if prio_mode else "110") or (130 if prio_mode else 110)))
        reg_reserve = min(len(regular), max(10, int(max_checks * REGULAR_MIN_SHARE))) if regular else 0
        urgent_cap = max(10, max_checks - reg_reserve)
        if len(urgent_all) > urgent_cap:
            keep = urgent_cap // 2
            rest = urgent_all[keep:]
            slots = urgent_cap - keep
            cur = _SEL_STATE["ucur"][lane] % max(1, len(rest))
            rot = [rest[(cur + j) % len(rest)] for j in range(min(slots, len(rest)))]
            _SEL_STATE["ucur"][lane] = (cur + len(rot)) % max(1, len(rest))
            urgent = urgent_all[:keep] + rot
        else:
            urgent = list(urgent_all)

        remaining = max(0, max_checks - len(urgent))
        rotated = []
        due_n = 0
        max_overdue = 0.0
        if regular and remaining:
            due = []
            for kv in regular:
                d = _NEXT_CHK.get(kv[0], 0.0)
                if d <= now_ts:
                    due.append((d, kv))
            due_n = len(due)
            if due_n > remaining:
                scored = []
                for d, kv in due:
                    iv = max(1.0, _mon_interval(kv[1], _age_seconds(kv[1])))
                    scored.append(((now_ts - max(d, now_ts - 86400)) / iv, d, kv))
                scored.sort(key=lambda x: -x[0])
                due = [(d, kv) for _, d, kv in scored]
            else:
                due.sort(key=lambda x: x[0])
            if due:
                max_overdue = now_ts - max(min(d for d, _ in due), now_ts - 86400)
            rotated = [kv for _, kv in due[:remaining]]
            for k, c in rotated:
                _NEXT_CHK[k] = now_ts + _mon_interval(c, _age_seconds(c))
            if len(_NEXT_CHK) > 3 * max(1000, total_before):
                live = {kv[0] for kv in regular}
                for k_ in [k for k in _NEXT_CHK if k not in live][:5000]:
                    _NEXT_CHK.pop(k_, None)
        items = urgent + rotated

        if now_ts - _SEL_STATE["log_at"][lane] >= 60:
            _SEL_STATE["log_at"][lane] = now_ts
            logger.info(
                f"monitor[{lane}]: tracked={total_before} hot={len(hot)} warm={len(warm)} "
                f"regular={len(regular)} (due now={due_n}, max overdue~{max_overdue:.0f}s) "
                f"glitch_wait={glitch_skipped} | this tick: urgent={len(urgent)} regular_checked={len(rotated)}")

        # Real-time bulk refresh (one DexScreener request per 30 tokens).
        try:
            global _DEX_BULK_CURSOR
            sel_set = set()
            urgent_keys = {k for k, _ in urgent}
            urgent_cas = [v.get("ca") for k, v in items
                          if v.get("ca") and not v.get("frozen") and k in urgent_keys]
            regular_cas = [v.get("ca") for k, v in items
                           if v.get("ca") and not v.get("frozen") and k not in urgent_keys]
            chain_map = {v.get("ca"): v.get("chain", "SOL")
                         for _, v in items if v.get("ca") and not v.get("frozen")}
            urgent_chunks = [urgent_cas[i:i + 30] for i in range(0, len(urgent_cas), 30)]
            regular_chunks = [regular_cas[i:i + 30] for i in range(0, len(regular_cas), 30)]
            max_batches = max(1, int(os.environ.get("MONITOR_BULK_MAX_BATCHES", "3") or 3))
            selected = list(urgent_chunks)
            remaining_budget = max(1 if regular_chunks else 0, max_batches - len(urgent_chunks))
            if regular_chunks and remaining_budget:
                start = _DEX_BULK_CURSOR % len(regular_chunks)
                more = [regular_chunks[(start + j) % len(regular_chunks)]
                        for j in range(min(remaining_budget, len(regular_chunks)))]
                _DEX_BULK_CURSOR = (start + len(more)) % len(regular_chunks)
                selected += more
            sel_set = {ca for ch in selected for ca in ch}
            if selected:
                await asyncio.gather(*[
                    asyncio.to_thread(_bulk_refresh_dex_sync, ch, 0, chain_map) for ch in selected
                ], return_exceptions=True)
        except Exception as e_bulk:
            logger.warning(f"bulk dex refresh failed: {e_bulk}")

        try:
            rn = time.time()
            for k, c in items:
                if k in _NEXT_CHK and c.get("ca") and _get_cached_dex(c.get("ca")) is None:
                    _NEXT_CHK[k] = min(_NEXT_CHK[k], rn + 4.0)
        except Exception:
            pass

        # Hot-call fast lane (age <= 180s, not already refreshed this tick).
        try:
            hot_list = []
            hot_chain_map = {}
            for hk, hc in items:
                if hc.get("frozen") or not hc.get("ca"):
                    continue
                if hc.get("ca") in sel_set:
                    continue
                try:
                    age = (datetime.utcnow() - datetime.fromisoformat(
                        str(hc.get("tracked_since", "")))).total_seconds()
                except Exception:
                    age = 999999
                if age <= 180:
                    hot_list.append((hk, hc))
                    hot_chain_map[hc.get("ca")] = hc.get("chain", "SOL")
                if len(hot_list) >= 40:
                    break
            if hot_list:
                await asyncio.to_thread(
                    _bulk_refresh_dex_sync,
                    [hc.get("ca") for _, hc in hot_list],
                    0, hot_chain_map)
        except Exception as e_hot:
            logger.debug(f"hot-call refresh failed: {e_hot}")

        sem = asyncio.Semaphore(
            max(1, int(os.environ.get("MONITOR_FALLBACK_CONCURRENCY", "6") or 6)))
        base_fb_max = max(0, int(os.environ.get(
            "MONITOR_PRIORITY_MAX_FALLBACKS_PER_TICK" if prio_mode else "MONITOR_MAX_FALLBACKS_PER_TICK",
            "16" if prio_mode else "10") or (16 if prio_mode else 10)))
        storm_boost = 1
        try:
            if _dex_breaker_open():
                storm_boost = 4
        except Exception:
            pass
        ctx = {
            "fb": {"used": 0, "max": base_fb_max * storm_boost},
            "sem": sem,
            "fb_timeout": max(1.0, float(os.environ.get("MONITOR_FALLBACK_TIMEOUT_SECONDS", "4") or 4)),
            "alt_timeout": max(1.0, float(os.environ.get("MONITOR_ALT_TIMEOUT_SECONDS", "3") or 3)),
        }
        for i in range(0, len(items), 30):
            await asyncio.gather(*[monbot_check_one(k, v, ctx) for k, v in items[i:i + 30]])
        _maybe_save_peaks()
        _maybe_save_dip()
        if not prio_mode:
            _prune_queue()
    except Exception as e:
        logger.error(f"monbot tick crash: {e}")


# === MAIN ===================================================================
async def _prio_loop(stop_event):
    while not stop_event.is_set():
        t0 = time.time()
        try:
            await asyncio.wait_for(monbot_tick(True), timeout=MONITOR_TICK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.error("monbot: prio tick hard-timeout hit - aborting this tick")
        except Exception as e:
            logger.error(f"monbot: prio tick crash: {e}")
        dt = MONBOT_PRIO_SECONDS - (time.time() - t0)
        if dt > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=dt)
            except asyncio.TimeoutError:
                pass


async def _bulk_loop(stop_event):
    while not stop_event.is_set():
        t0 = time.time()
        try:
            await asyncio.wait_for(monbot_tick(False), timeout=MONITOR_TICK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.error("monbot: bulk tick hard-timeout hit - aborting this tick")
        except Exception as e:
            logger.error(f"monbot: bulk tick crash: {e}")
        dt = MONBOT_TICK_SECONDS - (time.time() - t0)
        if dt > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=dt)
            except asyncio.TimeoutError:
                pass


async def _amain():
    _load_own_files()
    _reload_main_data()
    n_calls = len(_calls)
    n_ch = len({(c.get("channel") or "").lower() for c in _calls.values() if c.get("channel")})
    logger.info(
        f"monbot LIVE - monitoring X milestones: tracked={n_calls} "
        f"active_channels={n_ch} queued={len(_queue)} "
        f"(prio every {MONBOT_PRIO_SECONDS}s, bulk every {MONBOT_TICK_SECONDS}s, "
        f"bulk lane every 5th tick)")
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    await asyncio.gather(_prio_loop(stop_event), _bulk_loop(stop_event))
    _maybe_save_peaks(force=True)
    _maybe_save_dip(force=True)
    logger.info("monbot stopped.")


def main():
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        logger.info("monbot stopped (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
