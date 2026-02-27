# Hetzner 24/7 Deployment Guide

Step-by-step to run Trading Galaxy on a Hetzner Cloud VPS with automatic TLS,
systemd-managed Docker, and zero-downtime updates via `git pull && docker compose up --build -d`.

---

## 1. Recommended Server

| Spec | Value | Notes |
|---|---|---|
| Type | **CX22** (2 vCPU, 4 GB RAM) | Without Ollama — cheapest that runs comfortably |
| Type | **CX32** (4 vCPU, 8 GB RAM) | With Ollama (llama3.2 3B needs ~4 GB RAM) |
| OS | **Ubuntu 24.04 LTS** | |
| Region | Anywhere close to your users (nbg1 = Nuremberg, fsn1 = Falkenstein) |
| Firewall | Allow TCP 22, 80, 443 only | Block 5050, 11434 — Caddy handles all ingress |
| Backups | Enable (+20% cost) | Hetzner snapshots the whole volume |

Create at: https://console.hetzner.cloud

---

## 2. Point your domain

In your DNS provider, add an **A record**:

```
api.yourdomain.com  →  <hetzner-server-ipv4>
```

Wait for propagation (usually < 5 min with low TTL). Caddy won't get a TLS cert
until DNS resolves.

---

## 3. First-time server setup

SSH in as root, then run these once:

```bash
# Update OS
apt update && apt upgrade -y

# Install Docker (official script)
curl -fsSL https://get.docker.com | sh

# Install Docker Compose plugin (included in modern Docker, verify)
docker compose version

# Create a non-root deploy user
useradd -m -s /bin/bash deploy
usermod -aG docker deploy

# Copy your SSH key to the deploy user
mkdir -p /home/deploy/.ssh
cp ~/.ssh/authorized_keys /home/deploy/.ssh/
chown -R deploy:deploy /home/deploy/.ssh
chmod 700 /home/deploy/.ssh

# Switch to deploy user for all subsequent steps
su - deploy
```

---

## 4. Clone the repo

```bash
cd ~
git clone https://github.com/Alby2007/Mem-Backend.git trading-galaxy
cd trading-galaxy
```

---

## 5. Configure environment

```bash
cp .env.example .env
nano .env
```

Set **all** of these — the app will start with defaults but security will be broken:

```bash
# Required — generate fresh values:
JWT_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
DB_ENCRYPTION_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# Paste them into .env:
JWT_SECRET_KEY=<output from above>
DB_ENCRYPTION_KEY=<output from above>

# Required if using FRED macro data:
FRED_API_KEY=your_fred_api_key

# Required for Telegram briefing delivery:
TELEGRAM_BOT_TOKEN=your_telegram_bot_token

# Optional — only needed on this machine if you want to push seed updates:
# GITHUB_TOKEN=ghp_your_token
```

---

## 6. Configure Caddy

Edit `deploy/Caddyfile` and replace the domain placeholder:

```bash
sed -i 's/api.yourdomain.com/api.YOURACTUALDOMAIN.com/g' deploy/Caddyfile
```

Verify it looks right:

```bash
grep -n "yourdomain" deploy/Caddyfile   # should return nothing
```

---

## 7. Start the stack

### Without Ollama (API + Caddy only — recommended first)

```bash
docker compose up -d --build
```

> The `ollama` service has `profiles: ["llm"]` — it is **not** started unless
> you explicitly include the profile. The app falls back gracefully when Ollama
> is unavailable (LLM features disabled, all data endpoints fully functional).

### With Ollama

```bash
docker compose --profile llm up -d --build
```

Then pull a model into Ollama (run once, ~2 GB download):

```bash
docker compose exec ollama ollama pull llama3.2
docker compose exec ollama ollama pull phi3        # extraction model
```

---

## 8. Load the KB seed (first boot only)

The seed ships 378k historical calibration samples and regime history so interns
get established confidence levels from day one rather than "insufficient data".

```bash
bash deploy/seed-bootstrap.sh
```

This downloads the latest `kb_seed.sql` from GitHub Releases, loads it into the
`kb-data` Docker volume, and writes a flag file so it won't re-run on restarts.

Verify:

```bash
curl https://api.yourdomain.com/health
curl https://api.yourdomain.com/stats
```

Expected `/health` response:
```json
{"status": "ok", "db": "trading_knowledge.db", "facts": 8890}
```

---

## 9. systemd auto-start (survive reboots)

Create `/etc/systemd/system/trading-galaxy.service`:

```bash
sudo tee /etc/systemd/system/trading-galaxy.service > /dev/null << 'EOF'
[Unit]
Description=Trading Galaxy KB API
Requires=docker.service
After=docker.service network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=deploy
WorkingDirectory=/home/deploy/trading-galaxy
# Change --profile llm to include Ollama
ExecStart=/usr/bin/docker compose up -d --remove-orphans
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=120
TimeoutStopSec=30
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable trading-galaxy
sudo systemctl start trading-galaxy
```

---

## 10. Zero-downtime deploys

SSH in and run:

```bash
cd ~/trading-galaxy
git pull
docker compose up -d --build --remove-orphans
```

Docker rebuilds only the changed layer (usually the `COPY . .` layer — ~10s).
Caddy's upstream health check detects the brief restart and queues requests.
No traffic is dropped for sub-second restarts; for longer rebuilds Caddy holds
connections up to the proxy timeout.

Add this as a Makefile target:

```bash
make deploy   # see Makefile — already wired
```

---

## 11. Automatic nightly updates (optional)

Add a cron job to pull and redeploy nightly at 2 AM UTC:

```bash
crontab -e
```

```
0 2 * * * cd /home/deploy/trading-galaxy && git pull && docker compose up -d --build --remove-orphans >> /var/log/trading-galaxy-deploy.log 2>&1
```

---

## 12. DB backup

The KB lives in the `kb-data` Docker volume at `/data/trading_knowledge.db`.

**Manual snapshot:**
```bash
docker run --rm \
  -v trading-galaxy_kb-data:/data:ro \
  -v $(pwd)/backups:/backup \
  alpine \
  tar czf /backup/kb-$(date +%Y%m%d-%H%M).tar.gz -C /data .
```

**Nightly cron backup to Hetzner Object Storage or local:**
```
0 3 * * * docker run --rm \
  -v trading-galaxy_kb-data:/data:ro \
  -v /home/deploy/backups:/backup \
  alpine tar czf /backup/kb-$(date +\%Y\%m\%d).tar.gz -C /data . \
  && find /home/deploy/backups -name 'kb-*.tar.gz' -mtime +7 -delete
```

---

## 13. Monitoring

**Check all containers are healthy:**
```bash
docker compose ps
```

**Tail logs:**
```bash
docker compose logs -f trading-galaxy    # API logs
docker compose logs -f caddy             # Access + TLS logs
docker compose logs -f ollama            # LLM logs (if running)
```

**One-line health check:**
```bash
watch -n 5 'curl -sf https://api.yourdomain.com/health | python3 -m json.tool'
```

---

## 14. Server sizing reference

| Scenario | RAM needed | Recommended VPS |
|---|---|---|
| API + Caddy only (no LLM) | ~500 MB | CX22 (4 GB) — comfortable |
| + Ollama llama3.2 (3B) | ~4.5 GB | CX32 (8 GB) — snug |
| + Ollama llama3.1 (8B) | ~8 GB | CX42 (16 GB) |
| Full production load | scale horizontally | Put DB on separate volume, add load balancer |

For the intern launch, **CX22 without Ollama** is the right call. You get the
full data pipeline (yfinance, FRED, EDGAR, FCA, pattern detection, calibration)
with no LLM cost. The LLM layer can be added later on a larger box or pointed at
a remote Ollama instance via `OLLAMA_BASE_URL`.

---

## 15. Quick-reference command cheatsheet

```bash
# SSH in
ssh deploy@<hetzner-ip>

# Status
docker compose ps
systemctl status trading-galaxy

# Deploy update
cd ~/trading-galaxy && git pull && docker compose up -d --build

# Restart single service
docker compose restart trading-galaxy

# Reload Caddy config without downtime
docker compose exec caddy caddy reload --config /etc/caddy/Caddyfile

# Open a shell inside the API container
docker compose exec trading-galaxy bash

# Run the historical calibration inside the container
docker compose exec trading-galaxy python -m analytics.historical_calibration --years 3

# Re-bootstrap seed (after a DB wipe)
bash deploy/seed-bootstrap.sh --force

# View DB stats
curl https://api.yourdomain.com/stats | python3 -m json.tool
```
