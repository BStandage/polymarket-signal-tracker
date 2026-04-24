# Windows laptop — whale-bot setup

End-to-end guide for running the signal detector + trade executor on a
Windows laptop (ASUS ROG or similar), routing through Mullvad VPN, with
Discord notifications.

Total time: ~30 minutes if you have Coinbase set up. ~60 min otherwise.

---

## 0. One-time laptop prep

### Power settings (critical)
1. **Settings → System → Power & battery**
   - Screen and sleep → "On battery power, turn off after": Never
   - Screen and sleep → "When plugged in, turn off after": Never
   - Sleep settings: Never (both)
2. **Control Panel → Power Options → Choose what closing the lid does**
   - On battery: Do nothing
   - Plugged in: Do nothing
3. Keep the laptop plugged in, lid closed, on a flat ventilated surface.

### Auto-login (so bot survives reboots)
1. Win+R → `netplwiz` → uncheck "Users must enter a username and password to use this computer"
2. Enter your password when prompted
3. Reboot to verify it auto-logs in.

### Disable Windows Update auto-restart
Group Policy Editor → Computer Configuration → Administrative Templates → Windows Components → Windows Update → Configure Automatic Updates → "Notify for download and auto install" (or 2). Prevents reboots during trading hours.

---

## 1. Install prerequisites

### Python 3.11+
https://www.python.org/downloads/
- **Check "Add python.exe to PATH"** on the first install screen
- Verify: open PowerShell → `python --version` → should be 3.11.x or higher

### Git
https://git-scm.com/download/win
- Accept defaults
- Verify: `git --version`

### Mullvad VPN
https://mullvad.net/en/download/vpn/windows
- Buy time on the account page (5€/month or pay with crypto)
- Settings → **Auto-connect**: ON
- Settings → **Launch app on start-up**: ON
- Settings → **Always require VPN** (kill-switch): ON
- Settings → **Local network sharing**: OFF (safer)
- Choose a server in: **Canada (not Ontario)**, Sweden, or Switzerland

### Test VPN before continuing
- Connect Mullvad
- Visit https://mullvad.net/en/check → should say "You are secure" and show the VPN exit IP
- Now visit https://polymarket.com → should load, NOT show "view-only" blocker

---

## 2. Clone the repo

Open PowerShell (not as admin):
```powershell
cd $HOME
git clone https://github.com/BStandage/polymarket-signal-tracker.git whale-bot
cd whale-bot
```

## 3. Set up the Python environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r scripts\requirements.txt
```

If PowerShell blocks the activate script:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
then try again.

## 4. Create the `.env` file

```powershell
copy .env.example .env
notepad .env
```

Fill in at minimum:
```
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... (your webhook)
MODE=paper
```

**Leave MODE=paper for the first 3-5 days.** This runs everything end-to-end but places no real orders. You validate:
- VPN stays up
- Signals arrive
- Discord pings work
- "Would-have-traded" log makes sense

## 5. Create a Polymarket wallet (for later — skip if paper-testing only)

See [README.md's wallet section](../README.md) — you need:
1. Rabby extension in Chrome
2. Create new wallet, back up seed phrase on paper
3. Connect Rabby to polymarket.com (via VPN)
4. Fund it with $100 USDC on Polygon (Coinbase → Send → Polygon network)
5. Make one $5 manual trade on Polymarket to confirm everything works

Once confirmed, export the private key from Rabby:
- Rabby → account icon → Manage Addresses → the three dots next to your address → "Show private key"
- Add to `.env`:
  ```
  POLYMARKET_PRIVATE_KEY=0x...
  POLYMARKET_FUNDER_ADDRESS=0x... (the same wallet address)
  ```

## 6. Smoke-test

With venv activated:
```powershell
python scripts\trade_executor.py
```

You should see:
```
=== trade_executor starting ===
mode=paper bankroll=$100.00 max_concurrent=3 ...
```

Let it run for a minute, then check Discord — if any signals came through, you'll see embeds. Press Ctrl+C to stop.

If no signals appear, that's normal — we filter strictly and the watchlist is quiet sometimes. Check `scripts/pipeline.log` for polling activity.

## 7. Install as a background service (so it survives logout/reboot)

### Option A — Task Scheduler (built-in, easiest)

Save this file as `scripts/register_task.ps1`:
```powershell
$action  = New-ScheduledTaskAction -Execute "$PWD\.venv\Scripts\python.exe" `
             -Argument "scripts\trade_executor.py" `
             -WorkingDirectory $PWD
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries `
             -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
             -StartWhenAvailable
Register-ScheduledTask -TaskName "WhaleBot" -Action $action -Trigger $trigger `
             -Settings $settings -Description "Polymarket whale-tailing bot"
```

Run it (one time):
```powershell
.\scripts\register_task.ps1
```

Verify:
- Win+R → `taskschd.msc` → Task Scheduler Library → WhaleBot should appear
- Right-click WhaleBot → Run → should start

To stop: right-click → End. To disable: right-click → Disable.

### Option B — NSSM (more robust, runs as a Windows service)

Download nssm.exe from https://nssm.cc then:
```powershell
nssm install WhaleBot "$PWD\.venv\Scripts\python.exe" "$PWD\scripts\trade_executor.py"
nssm set WhaleBot AppDirectory "$PWD"
nssm set WhaleBot AppStdout "$PWD\scripts\bot.log"
nssm set WhaleBot AppStderr "$PWD\scripts\bot.log"
nssm start WhaleBot
```

NSSM is what I'd actually use — survives reboots regardless of login, handles crashes automatically, cleaner logs.

## 8. Verify end-to-end

1. Close and reopen the laptop lid
2. Wait 5 minutes
3. Check Discord — if any ENTER signals fired, you'll see embeds
4. Check `scripts\bot.log` (NSSM) or Event Viewer (Task Scheduler) for runtime logs
5. Look at `docs/data/portfolio.json` — should show your bankroll and any paper trades

## 9. Going live (after 3-5 days of paper mode looking good)

1. Stop the service (`nssm stop WhaleBot` or end the scheduled task)
2. Edit `.env` — change `MODE=paper` to `MODE=live`, ensure wallet keys are set
3. Uncomment `py-clob-client` in `scripts/requirements.txt` and reinstall:
   ```
   pip install -r scripts/requirements.txt
   ```
4. Restart the service
5. Watch the first live order carefully — be ready to `nssm stop WhaleBot` or `touch HALT` in the project root if anything looks off

## Emergency kill switch

Any time, from any device with SSH/RDP access:
```powershell
cd $HOME\whale-bot
New-Item -ItemType File -Path .\HALT
```
This file's presence halts all new entries immediately. Existing positions still get monitored for exit (stop-loss, take-profit, whale-exit). Delete the file to resume new entries.

For a full stop (no new OR exit activity):
```powershell
nssm stop WhaleBot
```

## Common issues

- **"Polymarket view-only" error in bot logs** → VPN dropped or leaked. Check Mullvad is connected. Kill-switch should have prevented this if enabled.
- **"py-clob-client not installed"** → you set `MODE=live` but didn't uncomment/install the package. Either switch back to paper or install.
- **No signals ever** → our strict filters may be too tight for the current watchlist. Check `docs/data/live_signals.json` for total signal count vs ENTER count. Loosen `MAX_DRIFT` in `.env` if needed.
- **Bot crashed** → check `scripts/bot.log`. Discord should also have a crash-dump message.

## Monitoring remotely

- **Discord** — primary feed, every significant event pings
- **TeamViewer or RDP** — if you need to see the screen
- **Tailscale** — recommended: free, encrypted VPN mesh, gives you SSH-like access to your laptop from anywhere. Install on laptop + phone, you can `ssh laptop` from the phone to check logs on the fly.
