# monbot - WizardScan X-milestone monitor bot (SECOND bot) - Deploy Guide

## Kya hai yeh

`monbot.py` ek **standalone** X-milestone monitor bot hai. Sirf ONE kaam
karta hai: main bot ke tracked calls ko watch karke X milestones (aur
dip-profit milestones) DETECT karna aur unhe apni queue file me likhna.
Post karna iska kaam NAHI hai - post main bot karta hai.

**Yeh main bot ko REPLACE nahi karta.** Main bot posting, leaderboard,
dip alerts, champions, Most Shared, Binance, dropped calls (dropbot ke
saath) - sab kuch pehle ki tarah karta hai. Sirf X-milestone **detection**
ka load main bot se hata kar monbot ko de diya gaya hai, kyunki main bot
3,339 tracked calls par ~113% CPU par chal raha hai aur user channels
kam nahi karna chahta (free KOL listings 4 Dec 2026 tak chal rahi hain).

## Design (safe, dropbot pattern, no shared writes)

- **Monbot** (is folder me): main bot ki data files ko READ-ONLY padhta
  hai (`tracked_calls.json`, `sent_milestones.json`, `bot_config.json`,
  `channels.json` - `/opt/bot/data` se, mtime change par reload), live
  prices fetch karta hai (DexScreener / Birdeye / GeckoTerminal - wahi
  sources jo main bot use karta hai), X milestones detect karta hai
  (main bot ke `check_one` jaisi logic), aur hit hone par apni file
  `/opt/monbot/data/monbot_milestones.json` me likhta hai.
- **Main bot**: apni monitoring jobs band kar deta hai (neeche
  MONBOT_MODE flag), aur `monbot_import_job` se monbot ki queue file ko
  har **1 second** me READ karke X alert khud post karta hai (via
  `rt_enqueue_milestone`) aur milestone ko `sent_milestones.json` me
  seal karta hai. Import interval 1s is liye hai: local file read cheap
  hai, aur X-alert delay ~1s rehta hai (user wish: X alerts stay FAST).
- **Koi bhi file do processes nahi likhte.** Monbot sirf apni files
  likhta hai (`monbot_milestones.json`, `monbot_peaks.json`,
  `monbot_dip.json`). Main bot ki `tracked_calls.json` /
  `sent_milestones.json` / `bot_config.json` ko monbot kabhi nahi likhta
  (is baat ka test `monbot_test.py` group 3 me hai).
- **Monbot ke paas koi Telegram bot token ya userbot session nahi hai.**
  Yeh deliberate hai: main bot hi posting ka malik hai, aur do bots ke
  beech session share karne se ek disconnect ho jata hai (yeh wohi
  masla tha jo dropbot ko Nov 24 ko @Wizard_Scan session par hua tha).

## Files (is folder me)

| File | Size | Kya hai |
|---|---|---|
| `monbot.py` | 132,097 B | Poora bot (ek file). Live main-bot code se AST-extract kiya gaya. |
| `apply_monbot_mode_nov24.py` | 12,056 B | Main bot patch (x_alert.py + runtime.py, idempotent, .bak backup). |
| `requirements.txt` | 366 B | Python deps (sirf requests). |
| `monbot.env.example` | 1,272 B | `.env` template - isko `.env` me copy karo. |
| `monbot_test.py` | 20,411 B | Test harness (41 checks, no network, no Telegram). |
| `monbot-deploy.md` | | Yeh guide. |

## Monbot kya karta hai / kya NAHI karta

**Karta hai:**
- Main bot ke tracked calls ko lanes me check karna (hot har tick,
  warm har 2nd tick, regular rotation with per-call due intervals -
  wahi `_monitoring_job_body` selection logic, prio lane 1s + bulk
  lane 2s with 5th-tick gate = 10s effective).
- Bulk DexScreener refresh (30 CAs per request, rotating cursor) +
  hot-call fast lane + bounded per-token fallback (alert_pending /
  dex_pending / young calls only, per-token backoff + per-tick budget).
- Alt-source probe (sirf jab DexScreener ~1.0x par stuck ho), rug gate
  (HARD reasons skip, SOFT reasons record-only), spike-glitch guard,
  peak tracking (apni `monbot_peaks.json` me), entry-confirmation gate
  (fail-safe 120s, RH 420s).
- **GUARD rule:** milestone sirf tab queue hoti hai jab THIS tick ka
  LIVE ratio level par ya upar ho. Peak upar + live neeche = is tick
  queue nahi (token recover kare to baad me fire hoga, warna kabhi nahi
  - stale alerts nahi aate).
- Dip-profit detection (wahi low-tracking rules: threshold, dust
  floor, confirm ticks, 20%+ deeper low par cycle reset) - due dip
  milestones bhi queue file me jati hain, post main bot karta hai.
- Queue prune har bulk tick par: sealed entries, orphaned call_keys,
  aur 24h se purani entries nikal di jati hain.

**NAHI karta hai:**
- Telegram par kuch bhi post nahi karta (no token, no session).
- Main bot ki koi file nahi likhta (tracked_calls.json,
  sent_milestones.json, milestone_posts.json, bot_config.json).
- Entry fill / reconcile nahi karta (dropbot + main bot ka kaam hai).
- `_confirm_entry_mc` task, early-rebase guard, failed-call point
  deduction, hot-streak registration, series sampling writes, watchdog
  dropped-alert requeue - yeh sab main bot par rehte hain ya v1 me
  jaan-boojh kar chhode gaye hain (neeche Known gaps dekho).

## VPS deploy steps (exact, isi order me)

### Step 1 - Repo (pehle, GitHub par)

Repo abhi bana NAHI hai. User ko GitHub par naya repo banana hai
(`realfarhad55-art/monbot` ya jo naam woh choose kare), phir is folder
ki files wahan upload karni hain:
- `monbot.py`
- `requirements.txt`
- `monbot.env.example`
- `apply_monbot_mode_nov24.py`
- `monbot_test.py`
- `monbot-deploy.md` (yeh guide)

**Note:** raw URLs (raw.githubusercontent.com) me repo ka ASLI naam
aana chahiye - jab tak repo nahi banta, koi raw URL kaam nahi karegi.
Is guide me koi raw URL is liye nahi di gayi.

### Step 2 - Folder + venv (one-time)

```bash
mkdir -p /opt/monbot/data
cd /opt/monbot
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### Step 3 - Config + channels copy karo (main bot se, fallback ke liye)

```bash
cp /opt/bot/data/bot_config.json /opt/monbot/data/bot_config.json
cp /opt/bot/data/channels.json /opt/monbot/data/channels.json
cp /opt/bot/data/priority_channels.json /opt/monbot/data/priority_channels.json 2>/dev/null || true
```

Notes:
- Monbot config/channels ko **live** `/opt/bot/data` se READ-ONLY
  padhta hai (mtime change par reload) - is liye in copies ko dobara
  copy karne ki zaroorat NAHI hoti jab main bot ki config ya channel
  list badle. Yeh copies sirf **fallback** hain: agar kabhi live file
  missing ho to monbot inhe use karega.
- `bot_config.json` me `custom_milestones` aur `dip_threshold_pct`
  hain - monbot wahi values use karta hai jo main bot use karta hai.

### Step 4 - .env banao

```bash
cp /opt/monbot/monbot.env.example /opt/monbot/.env
nano /opt/monbot/.env
```

Bharo:
- `BIRDEYE_API_KEY=` - main bot ki `/opt/bot/.env` wali key hi daalo
  (alt sources ke liye chahiye).
- `MORALIS_API_KEY=` / `CMC_API_KEY=` - wahi, main bot wali keys.
- Baaki defaults theek hain (data dir, tick intervals).

**Token/key kabhi chat me nahi - sirf VPS par `.env` me. Aur yaad
rakho: monbot ke liye koi Telegram token / session chahiye hi nahi.**

### Step 5 - Main bot patch + restart (monbot start karne se PEHLE)

```bash
cd /opt/bot
python3 apply_monbot_mode_nov24.py /opt/bot
echo MONBOT_MODE=1 >> /opt/bot/.env
systemctl restart bot.service
```

Patch kya karta hai (5 edits, 2 files, idempotent, `.bak_monbotmode_nov24`
backup banta hai):
- **E1** (`x_alert.py`): `monbot_import_job` add karta hai - monbot ki
  queue file har 1s me read karta hai, GUARD re-check karta hai
  (live ratio abhi bhi level par hai ya nahi - neeche ho to seal karke
  discard), phir seal-first + `rt_enqueue_milestone` se post karta hai.
  Dip entries ke liye `send_dip_alert` call karta hai.
- **E2** (`x_alert.py`): `monitoring_job` MONBOT_MODE=1 par turant
  return karta hai (detection band).
- **E3** (`x_alert.py`): `ath_backfill_job` aur `_bulk_lane_watchdog_job`
  bhi MONBOT_MODE=1 par return karte hain. **Watchdog band karna
  CRITICAL hai:** woh 45s tak monitoring tick na aane par `os._exit(1)`
  karta hai - band na karo to main bot restart-loop me phans jayega
  (uski apni monitoring to band hai).
- **E4** (`runtime.py`): `monbot_import_job` register karta hai -
  `interval=1, first=5` (har 1 second, pehli run 5s baad).

**WARNING - live file risk:** patch ke anchors Nov 23 ke base copies
(`x_alert-3aa26821.py`, `runtime-c97fc7f1.py`) se verify kiye gaye hain
(har anchor count==1). Live `/opt/bot/x_alert.py` me Nov 23 shaam ke
patches bhi hain (FINAL / PERM / DIPFIX / GUARD / DIP_PE_FIX /
DIP_CHANNEL_DM / STOP_FORWARD2 / DIP_BOLD_NOV24) jo base copies me
NAHI hain. Agar patch chalane par koi anchor **FAIL** ho, to force mat
karo - output bhej do, anchor ko live file ke mutabiq adjust karna
hoga. Patch fail hone par kuch bhi write nahi karta (fail-safe).

### Step 6 - systemd service + monbot start (SAB SE LAST)

`/etc/systemd/system/monbot.service` banao:

```ini
[Unit]
Description=WizardScan X-milestone monitor bot (monbot)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/monbot
ExecStart=/opt/monbot/venv/bin/python monbot.py
Restart=on-failure
RestartSec=10
Environment=MONBOT_ENV=/opt/monbot/.env

[Install]
WantedBy=multi-user.target
```

Phir:

```bash
systemctl daemon-reload
systemctl enable monbot.service
systemctl start monbot.service
```

**Monbot ko main bot ke restart ke BAAD start karo** (step 5 ke baad),
taake gap me double-detection na ho. Import job detection ke ~1s ke
andar post karta hai.

### Order of operations (zaroori - warna duplicates ya gap)

1. **Main bot:** patch (step 5) + `MONBOT_MODE=1` + restart.
   Is point par main bot detection band kar dega, lekin posting import
   job se chalti rahegi (jab monbot start hoga).
2. **Monbot:** `systemctl start monbot.service` karo.
3. Dono ke beech ka gap (restart se monbot start tak) sirf 1-2 minute
   rakhna - us gap me aane wale X miss ho sakte hain (monbot ke paas
   tab tak koi peak history nahi hoti, woh pehle sight par
   tracked record se seed karta hai).

**Rollback (agar kuch galat ho):** main bot ki `.env` se
`MONBOT_MODE=1` hatao + restart karo - main bot wapas khud detection
karne lagega. Phir `systemctl stop monbot.service` karo. Dono ek saath
ON mat rakho (duplicate detection hogi - monbot + main bot dono
milestone dekhenge, halanki seal ki wajah se double post nahi hona
chahiye, phir bhi risk mat lo).

---

## Verification checklist

Monbot start karne ke baad:

```bash
systemctl status monbot.service --no-pager | head -10
journalctl -u monbot.service --since "5 minutes ago" --no-pager | tail -30
```

Dekhna chahiye:
- `monbot LIVE - monitoring X milestones: tracked=3339 ... queued=0`
  (tracked count main bot ke record jaisa hona chahiye)
- `monitor[bulk]: tracked=... hot=... warm=... regular=...` (har minute)
- `monbot: queued 2X $TOKEN @Channel peak=... live=...` jab koi
  milestone hit ho.

Phir ek test: kisi tracked call ke token ke 2X hone par:
- `/opt/monbot/data/monbot_milestones.json` me entry aani chahiye
  (type x, x_val 2, status queued).
- Main bot ki `journalctl -u bot.service` me
  `monbot_import: posted 2X $TOKEN @Channel` ~1s ke andar aana chahiye.
- Channel par X alert pehle ki tarah aani chahiye (video + premium
  emojis ke saath).

CPU check:

```bash
ps aux --sort=-%cpu | head -5
```

Dono bots alag processes hain - monbot ka CPU alag dikhega, main bot ka
alag. Monitoring load ab main bot par nahi aana chahiye.

Files check (monbot ne main bot ki files ko haath nahi lagaya):

```bash
ls -l --time-style=full-iso /opt/bot/data/tracked_calls.json /opt/bot/data/sent_milestones.json
ls -l --time-style=full-iso /opt/monbot/data/
```

Main bot ki files ka mtime sirf main bot ke apne writes se badalna
chahiye, monbot ke start hone se nahi.

## Troubleshooting

| Masla | Wajah / Fix |
|---|---|
| `monbot LIVE` nahi aa raha / service fail | `.env` ka path check karo (`MONBOT_ENV` ya `/opt/monbot/.env`). `journalctl -u monbot.service` me error dekho. `venv/bin/pip install -r requirements.txt` dobara chalao. |
| `tracked=0` dikh raha hai | `MONBOT_MAIN_DATA_DIR` galat hai ya `/opt/bot/data/tracked_calls.json` nahi mili. `.env` check karo. |
| Queue file ban rahi hai lekin alert post nahi ho raha | Main bot me patch apply nahi hua ya `MONBOT_MODE=1` set nahi hua, ya main bot restart nahi hua. `journalctl -u bot.service \| grep monbot_import` dekho. Agar kuch nahi to `grep MONBOT_MODE_NOV24 /opt/bot/x_alert.py /opt/bot/runtime.py` karo - dono me marker hona chahiye. |
| Duplicate X alerts (dono bots se) | Main bot ki monitoring band nahi hui: `.env` me `MONBOT_MODE=1` check karo + `systemctl restart bot.service`. Monbot + main-bot monitoring ek saath ON mat rakho. |
| Main bot restart-loop me hai (bar bar `Bot started!`) | `_bulk_lane_watchdog_job` band nahi hua - E3b apply nahi hua. Patch dobara chalao aur output dekho (anchor fail to force mat karo, report karo). |
| X alert late aa raha hai (delay zyada hai) | Import job ka interval check karo: `grep monbot_import_job /opt/bot/runtime.py` - `interval=1, first=5` hona chahiye. Agar `interval=3` dikhe to purana patch hai, naya patch chalao. |
| Stale alert post ho gaya (token dump ke baad bhi) | GUARD re-check import job me hai - check karo ke E1 wala `monbot_import_job` live file me hai aur `GUARD discard` wala code present hai. Monbot side par GUARD bhi hai (live < level = no queue). Dono me se koi ek bhi missing ho to report karo. |
| `monbot: GUARD hold ...` logs bar bar | Normal hai - token peak par gaya tha phir neeche aa gaya. Jab tak live wapas level par nahi aata, alert hold rahega. Yeh feature hai, bug nahi. |
| Monbot restart par purane milestones dobara queue ho rahe hain | Nahi hone chahiye - sealed milestones `sent_milestones.json` se skip hote hain aur queue prune hoti hai. Agar ho rahe hain to main bot ki `sent_milestones.json` me seal missing hai (import job ne seal nahi kiya) - `journalctl -u bot.service \| grep monbot_import` dekho. |

## Known gaps / limitations (v1)

1. **ATH backfill band hai (main bot par).** `ath_backfill_job` MONBOT_MODE
   me off hai, aur monbot ke paas candle-history backfill nahi hai. Jo
   token bot restart/offline ke doran pump hua, uska peak sirf live
   ticks se banta hai - purana peak wapas nahi aata. v2 me monbot me
   backfill add hoga.
2. **Early-rebase guard monbot me nahi hai.** Galat (bohot neeche) entry
   par free-X ka risk main bot ke guard se cover hota tha; monbot v1 me
   woh guard nahi chalata. Entry correctness dropbot / main bot ki
   zimmedari hai. Agar galat entry se free X dikhe to `/fixoldcall`
   se fix karo (main bot par).
3. **Entry-confirm fail-safe sirf gate hai.** Monbot `_confirm_entry_mc`
   task nahi chalata - unconfirmed entry wali call 120s (RH 420s) tak
   hold rehti hai, phir fail-safe se fire hoti hai (main bot jaisa).
   Confirm task ke baghair yeh delay main bot se lamba ho sakta hai.
4. **GUARD spec se implement kiya gaya hai, extracted code se nahi.**
   Live main bot ke GUARD_NOV23 patch ka code base copies me nahi tha,
   is liye GUARD rule brief ki spec se likha gaya hai (monbot side:
   live >= level tabhi queue; import job side: live < level to seal +
   discard). Behavior spec ke mutabiq hai, lekin live GUARD code se
   byte-match verify nahi hua.
5. **Entry fill / reconcile nahi hota.** `dex_pending` calls jinki
   entry abhi tak fill nahi hui, unka ratio 0 rehta hai aur monbot
   unhe skip karta hai - jab tak dropbot / main bot entry fill na kare.
6. **No series sampling writes.** Website chart ke liye price-history
   sampling monbot nahi karta (main bot ka kaam tha, band hai) - v1 me
   chart data update nahi hoga. Points / leaderboard par asar nahi
   (woh `peak_ratio` / `best_x` se bante hain, jo monbot queue karta hai).
7. **Failed-call point deduction nahi hoti.** 48h me 2X na aane par -10
   points wali deduction main bot ke monitoring body me thi; monbot v1
   me nahi hai. Leaderboard points par chhota asar ho sakta hai.
8. **Hot-streak registration nahi hoti.** Monbot `_hs_register_call`
   / `_hs_on_call_hit` nahi chalata - hot streak alerts v1 me monbot
   se trigger nahi honge.
9. **Config/channels fallback copies manual hain.** Live read primary
   hai (step 3 ki copies sirf fallback), is liye yeh gap chhota hai -
   lekin agar `/opt/bot/data` ka path badle to `.env` me
   `MONBOT_MAIN_DATA_DIR` update karna hoga.
10. **Patch anchors vs live files.** Anchors Nov 23 base copies se
    verify hain. Live files me post-Nov-23 patches hain; anchor fail
    hone par force mat karo (patch khud fail-safe hai - kuch write
    nahi karta).

## Build notes (kaise bana hai)

- `monbot.py` ka bada hissa **AST extraction** hai: Nov 23 ke base
  copies (`core-fadf8802.py`, `x_alert-3aa26821.py`) se zaroori
  functions/constants verbatim nikale gaye (124 names, dependency
  closure symtable se verify, 0 missing). Original file order rakha
  gaya hai (module-level assignments ek doosre par depend karte hain).
- Em-dash kahin nahi hai (code ya comments me) - plain hyphens hain.
- `monbot_test.py`: 41 checks, sab pass (no network, no Telegram):
  detection (1.9/2.0/2.1/5/10x, sealed sets, custom milestones),
  GUARD rule, no-write verification, queue format, import-job
  simulation, dip queueing + cycle reset, spike-glitch guard, peak
  seeding.
- `monbot.py` compile hota hai, symtable undefined globals = 0.
- Patch test: base-file copies par apply + dobara run (idempotent) +
  dono files compile + backups pristine - sab pass.
