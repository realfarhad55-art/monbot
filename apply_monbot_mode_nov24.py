"""apply_monbot_mode_nov24.py - Main bot changes for the separate monbot.

Monbot (separate X-milestone monitoring bot) takes over milestone DETECTION.
The main bot stops detecting and only POSTS alerts via monbot_import_job,
which reads monbot's own /opt/monbot/data/monbot_milestones.json every 1s.

Four edits, two files (run from /opt/bot):

E1 - x_alert.py: add monbot_import_job + MONBOT_QUEUE_FILE constant,
     inserted BEFORE `async def monitoring_job(...)`.
E2 - x_alert.py: monitoring_job early return when MONBOT_MODE=1.
E3 - x_alert.py: ath_backfill_job + _bulk_lane_watchdog_job early returns
     when MONBOT_MODE=1. The watchdog MUST be disabled: it calls os._exit(1)
     if no monitoring tick starts for 45s, so without this the main bot
     would restart-loop after the switch (its own monitoring is off).
E4 - runtime.py: register monbot_import_job (every 1s, first run 5s).
     Interval is 1s on purpose: polling a local queue file is cheap, and
     it keeps the X-alert delay at ~1s instead of ~3s (user wish: X alerts
     stay FAST).

Plus add MONBOT_MODE=1 to /opt/bot/.env, then restart the main bot.
Rollback: remove MONBOT_MODE=1 from .env + restart main bot, and
systemctl stop monbot.service. Never run monbot + main-bot monitoring
at the same time (duplicate detection).

Idempotent. Backs up x_alert.py to x_alert.py.bak_monbotmode_nov24
and runtime.py to runtime.py.bak_monbotmode_nov24.
Run: python3 apply_monbot_mode_nov24.py [/opt/bot]
Then: echo MONBOT_MODE=1 >> /opt/bot/.env && systemctl restart bot.service

WARNING: live /opt/bot/x_alert.py has post-Nov-23 patches (GUARD / FINAL /
PERM / DIPFIX / DIP_BOLD) that are NOT in the Nov 23 base copies this patch
was written against. If any anchor below FAILS on the live files, do NOT
force it - report back instead. Anchors were verified count==1 against the
Nov 23 base copies (x_alert-3aa26821.py, runtime-c97fc7f1.py).
"""

import os, sys, shutil

MARKER = "MONBOT_MODE_NOV24"

# --- E1: monbot import job (inserted before monitoring_job) ---
OLD1 = '''async def monitoring_job(context: ContextTypes.DEFAULT_TYPE):'''

NEW1 = '''# MONBOT_MODE_NOV24: monbot ki queued milestones import karo + X alert post karo.
# Monbot (separate bot) sirf DETECT karta hai aur apni file
# /opt/monbot/data/monbot_milestones.json me likhta hai. Yeh job woh file
# har 1 second me READ karke main bot ke tracked_calls / sent_milestones
# se match karta hai, GUARD re-check karta hai, phir alert post karta hai.
# Main bot hi posting + record writes ka malik hai. Monbot kabhi post nahi
# karta aur uske paas koi Telegram token / userbot session nahi hai.
MONBOT_QUEUE_FILE = "/opt/monbot/data/monbot_milestones.json"


async def monbot_import_job(context):
    import json
    try:
        with open(MONBOT_QUEUE_FILE, encoding="utf-8") as f:
            queue = json.load(f) or {}
    except FileNotFoundError:
        return  # monbot abhi start nahi hua
    except Exception as e:
        logger.warning(f"monbot_import: read failed: {e}")
        return
    if not isinstance(queue, dict) or not queue:
        return
    try:
        entries = sorted(queue.values(), key=lambda e: str(e.get("detected_at", "")))
    except Exception:
        entries = list(queue.values())
    for entry in entries[:10]:  # max 10 per run, baaki agle second me
        try:
            if not isinstance(entry, dict):
                continue
            call_key = entry.get("call_key", "")
            typ = entry.get("type", "x")
            x_val = int(entry.get("x_val", 0) or 0)
            call = tracked_calls.get(call_key)
            if not call or x_val <= 0:
                continue
            if typ == "dip":
                sent_dip = call.get("sent_dip_milestones")
                if not isinstance(sent_dip, list):
                    sent_dip = []
                    call["sent_dip_milestones"] = sent_dip
                if x_val in sent_dip:
                    continue
                try:
                    call["dip_low"] = float(entry.get("dip_low", 0) or 0)
                except (TypeError, ValueError):
                    pass
                sent_dip.append(x_val)
                _save_tracked()
                try:
                    ok = await send_dip_alert(context.bot, call_key, call, x_val,
                                              float(entry.get("cur_mc", 0) or 0))
                except Exception as e:
                    logger.warning(f"monbot_import: dip send failed {x_val}X {call_key}: {e}")
                    ok = False
                if ok:
                    logger.info(f"monbot_import: dip posted {x_val}X {call_key}")
                else:
                    # send_dip_alert ne isay pending_dip me daal diya hai
                    logger.info(f"monbot_import: dip held {x_val}X {call_key}")
                continue
            # X entry
            sealed = sent_milestones.get(call_key, set())
            if x_val in sealed:
                continue
            # GUARD re-check: live ratio abhi bhi level par hai ya nahi?
            # Fetch fail ho to proceed karo (GUARD fallback: fetch failure
            # par kabhi block nahi karna). Entry <= 0 ho to bhi proceed karo.
            try:
                live = await asyncio.wait_for(
                    fetch_dexscreener(call.get("ca", ""), allow_alt=False,
                                      retries=1, min_liquidity=0),
                    timeout=4)
            except Exception:
                live = None
            if live:
                try:
                    live_mc = float((live or {}).get("mcap", 0) or 0)
                    entry_mc = float(call.get("entry_mc", 0) or 0)
                except (TypeError, ValueError):
                    live_mc, entry_mc = 0.0, 0.0
                if entry_mc > 0 and live_mc > 0:
                    live_ratio = live_mc / entry_mc
                    if live_ratio < x_val:
                        # Stale alert: token level se neeche aa gaya. Seal
                        # karke discard karo (dobara queue nahi hoga).
                        sent_milestones.setdefault(call_key, set()).add(x_val)
                        _save_milestones()
                        logger.info(
                            f"monbot_import: GUARD discard {x_val}X "
                            f"{call.get('symbol', '?')} live {live_ratio:.2f}x")
                        continue
            # SEAL FIRST, phir post karo (restart par dobara post na ho).
            sent_milestones.setdefault(call_key, set()).add(x_val)
            _save_milestones()
            try:
                await rt_enqueue_milestone(
                    context.bot, call_key, call, x_val,
                    entry.get("ms_mc_fmt", "N/A"),
                    record_only=bool(entry.get("record_only", False)), delay=0)
            except Exception as e:
                logger.warning(f"monbot_import: enqueue failed {x_val}X {call_key}: {e}")
                continue
            logger.info(
                f"monbot_import: posted {x_val}X {call.get('symbol', '?')} "
                f"@{call.get('channel', '?')}")
        except Exception as e:
            logger.warning(f"monbot_import: skip entry: {e}")
            continue


async def monitoring_job(context: ContextTypes.DEFAULT_TYPE):'''

# --- E2: monitoring_job early return ---
OLD2 = '''    global _MON_RUNNING, _PRIO_MON_RUNNING, _MON_LAST_TICK_ENTERED, _PRIO_MON_LAST_TICK_ENTERED
    prio_mode = getattr(getattr(context, "job", None), "data", None) == "priority"'''

NEW2 = '''    global _MON_RUNNING, _PRIO_MON_RUNNING, _MON_LAST_TICK_ENTERED, _PRIO_MON_LAST_TICK_ENTERED
    prio_mode = getattr(getattr(context, "job", None), "data", None) == "priority"
    # MONBOT_MODE_NOV24: X-milestone detection moved to the separate monbot.
    # This bot only posts alerts via monbot_import_job.
    if os.environ.get("MONBOT_MODE") == "1":
        return'''

# --- E3a: ath_backfill_job early return ---
OLD3A = '''    global _ath_cursor
    try:
        items = [(k, v) for k, v in tracked_calls.items()'''

NEW3A = '''    global _ath_cursor
    # MONBOT_MODE_NOV24: ATH backfill is part of milestone detection, which
    # now lives in the separate monbot. Keep it off here so peaks are not
    # double-counted while monbot is running.
    if os.environ.get("MONBOT_MODE") == "1":
        return
    try:
        items = [(k, v) for k, v in tracked_calls.items()'''

# --- E3b: _bulk_lane_watchdog_job early return ---
OLD3B = '''    now = time.time()
    bulk_stuck_for = now - _MON_LAST_TICK_ENTERED'''

NEW3B = '''    # MONBOT_MODE_NOV24: monitoring is OFF on this bot by design, so the
    # bulk lane will never tick again. Without this return the watchdog
    # would call os._exit(1) after 45s and the bot would restart-loop.
    if os.environ.get("MONBOT_MODE") == "1":
        return
    now = time.time()
    bulk_stuck_for = now - _MON_LAST_TICK_ENTERED'''

# --- E4: register the import job in runtime.py (after userbot_watchdog_job) ---
OLD4 = '''    app.job_queue.run_repeating(userbot_watchdog_job, interval=45,        first=45)   # har 45s: userbot alive check + auto-reconnect (backstop for the immediate _kick_userbot_reconnect)'''

NEW4 = '''    app.job_queue.run_repeating(userbot_watchdog_job, interval=45,        first=45)   # har 45s: userbot alive check + auto-reconnect (backstop for the immediate _kick_userbot_reconnect)
    app.job_queue.run_repeating(monbot_import_job, interval=1, first=5)  # MONBOT_MODE_NOV24: monbot ki milestones import karo + X alert post karo (1s: local file read cheap hai, X alert fast rehta hai)'''


def patch_one(path, old, new, tag):
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    if src.count(old) != 1:
        print("FAIL: %s anchor missing/dup in %s (found %dx) -- NOT writing "
              "anything." % (tag, path, src.count(old)))
        return False
    bak = path + ".bak_monbotmode_nov24"
    if not os.path.exists(bak):
        # First edit only: keep a PRISTINE pre-patch backup (later edits
        # must not overwrite it, or rollback would restore a half-patched file).
        shutil.copy2(path, bak)
    src = src.replace(old, new, 1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(src)
    return True


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    xa = os.path.join(d, "x_alert.py")
    rt = os.path.join(d, "runtime.py")
    for p in (xa, rt):
        if not os.path.exists(p):
            print("FAIL: %s not found" % p)
            return 1
    with open(xa, encoding="utf-8") as f:
        xasrc = f.read()
    with open(rt, encoding="utf-8") as f:
        rtsrc = f.read()
    if MARKER in xasrc and "monbot_import_job" in xasrc and MARKER in rtsrc:
        print("OK: already has MONBOT_MODE_NOV24, skipping.")
        return 0
    if MARKER in xasrc or "monbot_import_job" in xasrc or MARKER in rtsrc:
        print("FAIL: partial state (marker in one file only) -- inspect "
              "x_alert.py / runtime.py before re-running.")
        return 1
    if not patch_one(xa, OLD1, NEW1, "E1"):
        return 1
    print("E1 OK (monbot_import_job added to x_alert.py)")
    if not patch_one(xa, OLD2, NEW2, "E2"):
        return 1
    print("E2 OK (monitoring_job OFF when MONBOT_MODE=1)")
    if not patch_one(xa, OLD3A, NEW3A, "E3a"):
        return 1
    print("E3a OK (ath_backfill_job OFF when MONBOT_MODE=1)")
    if not patch_one(xa, OLD3B, NEW3B, "E3b"):
        return 1
    print("E3b OK (_bulk_lane_watchdog_job OFF when MONBOT_MODE=1)")
    if not patch_one(rt, OLD4, NEW4, "E4"):
        return 1
    print("E4 OK (monbot_import_job registered in runtime.py, every 1s)")
    print("DONE. Backups: x_alert.py.bak_monbotmode_nov24, runtime.py.bak_monbotmode_nov24")
    print("Next: echo MONBOT_MODE=1 >> /opt/bot/.env && systemctl restart bot.service")
    print("Then: systemctl start monbot.service (LAST, after main bot restart)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
