#!/bin/bash
# Appends/updates Stripe env vars in /opt/trading-galaxy/.env
# Fill in STRIPE_SECRET_KEY and all STRIPE_PRICE_* before running.
ENV=/opt/trading-galaxy/.env

upsert() {
  grep -q "^${1}=" "$ENV" 2>/dev/null \
    && sed -i "s|^${1}=.*|${1}=${2}|" "$ENV" \
    || echo "${1}=${2}" >> "$ENV"
}

# ---- already known ----
upsert STRIPE_WEBHOOK_SECRET   "whsec_271LRsyPS4p8bXGX7wVvF4KSdGJebzDe"

# ---- fill these in ----
upsert STRIPE_SECRET_KEY       "sk_live_REPLACE_ME"
upsert STRIPE_PRICE_BASIC_M    "price_REPLACE_ME"
upsert STRIPE_PRICE_BASIC_A    "price_REPLACE_ME"
upsert STRIPE_PRICE_PRO_M      "price_REPLACE_ME"
upsert STRIPE_PRICE_PRO_A      "price_REPLACE_ME"
upsert STRIPE_PRICE_PREMIUM_M  "price_REPLACE_ME"
upsert STRIPE_PRICE_PREMIUM_A  "price_REPLACE_ME"

sudo systemctl restart trading-galaxy && sleep 2 && sudo systemctl is-active trading-galaxy
