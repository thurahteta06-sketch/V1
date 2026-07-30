# Voucher Scanner Telegram Bot

Telegram bot for brute-forcing Ruijie network vouchers.

## Commands
- `/setup <url>` – Set session URL
- `/brute <mode> <length> [target]` – Start scanning
  - Mode 1: Digits (0-9)
  - Mode 2: Lowercase (a-z)
  - Mode 3: Uppercase (A-Z)
  - Mode 4: Letters (a-zA-Z)
  - Mode 5: Alphanumeric (a-z0-9)
- `/status` – Show status
- `/stop` – Stop scan
- `/resume` – Resume scan
- `/saved` – Show found codes
- `/delete_saved` – Delete saved codes
- `/recheck` – Recheck saved codes
- `/notify on/off` – Toggle notifications

## Deploy to Railway
1. Fork this repo.
2. Create a new project on Railway.
3. Add environment variables: `BOT_TOKEN` and `ADMIN_ID`.
4. Deploy!
