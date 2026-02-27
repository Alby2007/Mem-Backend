#!/usr/bin/env bash
# deploy/oci-setup.sh
#
# One-shot server setup for Oracle Cloud Free Tier (Ubuntu 22.04 ARM/x86).
# Run as the default OCI user (ubuntu) immediately after first SSH.
#
# Usage:
#   ssh ubuntu@<your-oci-ip>
#   curl -fsSL https://raw.githubusercontent.com/Alby2007/Mem-Backend/master/deploy/oci-setup.sh | bash
#   -- OR clone first --
#   git clone https://github.com/Alby2007/Mem-Backend.git ~/trading-galaxy
#   bash ~/trading-galaxy/deploy/oci-setup.sh

set -euo pipefail

REPO_URL="https://github.com/Alby2007/Mem-Backend.git"
APP_DIR="$HOME/trading-galaxy"
VENV_DIR="$APP_DIR/.venv"
DATA_DIR="/opt/trading-galaxy/data"
SERVICE_NAME="trading-galaxy"
PYTHON="python3"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║        Trading Galaxy — OCI Server Setup             ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. System packages ────────────────────────────────────────────────────────
echo "[1/7] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    python3 python3-pip python3-venv python3-dev \
    git curl wget unzip build-essential \
    sqlite3 libsqlite3-dev \
    nginx certbot python3-certbot-nginx \
    ufw

echo "      Python: $($PYTHON --version)"

# ── 2. Clone / update repo ────────────────────────────────────────────────────
echo "[2/7] Cloning repository..."
if [[ -d "$APP_DIR/.git" ]]; then
    echo "      Repo already exists — pulling latest..."
    git -C "$APP_DIR" pull --ff-only
else
    git clone "$REPO_URL" "$APP_DIR"
fi

# ── 3. Python virtual environment + dependencies ──────────────────────────────
echo "[3/7] Creating virtualenv and installing dependencies..."
$PYTHON -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt" -q

# Install ollama (local LLM runtime)
if ! command -v ollama &>/dev/null; then
    echo "      Installing Ollama..."
    curl -fsSL https://ollama.com/install.sh | sh
    # Pull default model in background (can take a few minutes)
    echo "      Pulling llama3.2 model (background — check: ollama list)..."
    nohup ollama serve &>/tmp/ollama-setup.log &
    sleep 3
    ollama pull llama3.2 &>/tmp/ollama-pull.log &
    echo "      Ollama pulling in background. Run: tail -f /tmp/ollama-pull.log"
else
    echo "      Ollama already installed: $(ollama --version 2>/dev/null || echo 'ok')"
fi

# ── 4. Data directory ─────────────────────────────────────────────────────────
echo "[4/7] Setting up data directory..."
sudo mkdir -p "$DATA_DIR"
sudo chown "$USER:$USER" "$DATA_DIR"
chmod 750 "$DATA_DIR"

# ── 5. Environment file ───────────────────────────────────────────────────────
echo "[5/7] Environment file..."
ENV_FILE="$APP_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    echo "      .env already exists — skipping. Edit manually: $ENV_FILE"
else
    cp "$APP_DIR/.env.example" "$ENV_FILE"
    echo ""
    echo "  ⚠  IMPORTANT: Edit $ENV_FILE before starting the service!"
    echo "     At minimum set: JWT_SECRET_KEY, FRED_API_KEY, TELEGRAM_BOT_TOKEN"
    echo "     Then re-run: bash $APP_DIR/deploy/oci-setup.sh"
    echo ""
fi

# ── 6. Systemd service ────────────────────────────────────────────────────────
echo "[6/7] Installing systemd service..."
sudo cp "$APP_DIR/deploy/trading-galaxy.service" /etc/systemd/system/
sudo sed -i "s|__APP_DIR__|$APP_DIR|g"       /etc/systemd/system/trading-galaxy.service
sudo sed -i "s|__VENV_DIR__|$VENV_DIR|g"     /etc/systemd/system/trading-galaxy.service
sudo sed -i "s|__DATA_DIR__|$DATA_DIR|g"     /etc/systemd/system/trading-galaxy.service
sudo sed -i "s|__USER__|$USER|g"             /etc/systemd/system/trading-galaxy.service

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"

# ── 7. Firewall — allow 5050 (direct) and 443/80 (nginx) ─────────────────────
echo "[7/7] Configuring firewall..."
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 5050/tcp     # direct API access (restrict later behind nginx)
sudo ufw --force enable

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Setup complete. Next steps:                         ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  1. Edit .env:  nano $APP_DIR/.env"
echo "║  2. Start:      sudo systemctl start trading-galaxy  ║"
echo "║  3. Check:      sudo systemctl status trading-galaxy ║"
echo "║  4. Logs:       sudo journalctl -u trading-galaxy -f ║"
echo "║  5. Health:     curl http://localhost:5050/health     ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Also add your OCI Security List ingress rule:"
echo "  Destination port: 5050, Protocol: TCP, Source: 0.0.0.0/0"
echo ""
