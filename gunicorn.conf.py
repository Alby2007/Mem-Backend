# gunicorn.conf.py
#
# CURRENT STATE: Flask on :8000 (production traffic)
# FastAPI runs separately: uvicorn api_v2:app --host 0.0.0.0 --port 8001 --workers 2
#
# CUTOVER (Phase 8 complete): uncomment FastAPI block, comment Flask block, restart.

# ── Flask (current) ───────────────────────────────────────────────────────────
bind    = "0.0.0.0:8000"
workers = 2
threads = 4
wsgi_app = "api:app"

# ── FastAPI cutover (uncomment when all phases pass eval) ─────────────────────
# bind         = "0.0.0.0:8000"
# workers      = 2
# worker_class = "uvicorn.workers.UvicornWorker"
# wsgi_app     = "api_v2:app"
