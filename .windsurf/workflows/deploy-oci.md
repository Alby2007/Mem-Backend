---
description: Deploy Trading Galaxy KB API to Oracle Cloud (OCI) Free Tier VM
---

## Prerequisites
- OCI VM provisioned (Ubuntu 22.04, any shape — ARM Ampere A1 recommended for free tier)
- SSH key added during instance creation
- OCI Security List ingress rule: TCP port 5050 from 0.0.0.0/0

---

## Step 1 — SSH into the VM
Find your public IP in OCI Console → Compute → Instances → trading-galaxy → Details tab.

```bash
ssh ubuntu@<YOUR_OCI_PUBLIC_IP>
```

## Step 2 — Run the one-shot setup script
This installs Python, git, Ollama, creates the venv, installs deps, and registers the systemd service.

```bash
curl -fsSL https://raw.githubusercontent.com/Alby2007/Mem-Backend/master/deploy/oci-setup.sh | bash
```

Or if you prefer to clone first:
```bash
git clone https://github.com/Alby2007/Mem-Backend.git ~/trading-galaxy
bash ~/trading-galaxy/deploy/oci-setup.sh
```

## Step 3 — Configure environment variables
```bash
nano ~/trading-galaxy/.env
```

Minimum required values to set:
- `JWT_SECRET_KEY` — generate with: `python3 -c "import secrets; print(secrets.token_hex(32))"`
- `FRED_API_KEY` — free key from https://fred.stlouisfed.org/docs/api/api_key.html
- `EDGAR_USER_AGENT` — `TradingGalaxyKB your@email.com`
- `TRADING_KB_DB` — already set to `/opt/trading-galaxy/data/trading_knowledge.db`

Optional:
- `TELEGRAM_BOT_TOKEN` — for push notifications
- `DB_ENCRYPTION_KEY` — for at-rest encryption of sensitive atoms

## Step 4 — Start the service
```bash
sudo systemctl start trading-galaxy
sudo systemctl status trading-galaxy
```

## Step 5 — Verify it's running
```bash
# Local health check
curl http://localhost:5050/health

# From your laptop (uses public IP)
curl http://<YOUR_OCI_PUBLIC_IP>:5050/health

# Check ingest adapters are scheduled
curl http://localhost:5050/ingest/status

# Check KB atom count
curl http://localhost:5050/stats
```

## Step 6 — Load initial KB seed (optional but recommended)
If you have a `kb_seed.sql` exported from your local KB:
```bash
bash ~/trading-galaxy/deploy/seed-bootstrap.sh
```

## Step 7 — Wait for Ollama model download
The setup script pulls `llama3.2` in the background. Check progress:
```bash
tail -f /tmp/ollama-pull.log
# Or:
ollama list
```

Once the model is ready, test the full chat pipeline:
```bash
curl -s -X POST http://localhost:5050/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "what is the current market regime?", "session_id": "test"}' | python3 -m json.tool
```

---

## Updating the server after a code push

Run from the OCI VM:
```bash
bash ~/trading-galaxy/deploy/oci-update.sh
```

This does: `git pull` → `pip install -r requirements.txt` → `systemctl restart trading-galaxy`

---

## Useful commands on the VM

```bash
# Live logs
sudo journalctl -u trading-galaxy -f

# Restart
sudo systemctl restart trading-galaxy

# Stop
sudo systemctl stop trading-galaxy

# Check Ollama
ollama list
ollama serve   # if not running as a service

# DB size
ls -lh /opt/trading-galaxy/data/

# Run manual ingest
curl -X POST http://localhost:5050/ingest/run-all
```

---

## OCI Security List — required ingress rules

In OCI Console → Networking → Virtual Cloud Networks → your VCN → Security Lists:

| Direction | Protocol | Source     | Port | Description        |
|-----------|----------|------------|------|--------------------|
| Ingress   | TCP      | 0.0.0.0/0  | 22   | SSH                |
| Ingress   | TCP      | 0.0.0.0/0  | 5050 | Trading Galaxy API |
| Ingress   | TCP      | 0.0.0.0/0  | 80   | HTTP (nginx later) |
| Ingress   | TCP      | 0.0.0.0/0  | 443  | HTTPS (nginx later)|

---

## Notes
- The Free Tier ARM instance (VM.Standard.A1.Flex, 4 OCPUs / 24GB RAM) is more than enough for llama3.2
- The SQLite DB lives at `/opt/trading-galaxy/data/trading_knowledge.db` — back it up periodically
- Ingest adapters run automatically on schedule once the service starts
- The service auto-restarts on crash (RestartSec=10, up to 3 times per minute)
