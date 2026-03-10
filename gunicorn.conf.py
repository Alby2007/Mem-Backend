# gunicorn.conf.py  — FastAPI (cutover complete)
bind              = "0.0.0.0:5050"
workers           = 1   # SQLite can't handle concurrent writers — keep at 1
worker_class      = "uvicorn.workers.UvicornWorker"
wsgi_app          = "api_v2:app"
loglevel          = "info"
timeout           = 120       # kill + respawn worker if it stops responding for 120s
graceful_timeout  = 30        # time to finish in-flight requests on SIGTERM
keepalive         = 5         # reuse connections for 5s
worker_tmp_dir    = "/dev/shm"  # use tmpfs for worker heartbeat file (avoids disk I/O stall)
# Auto-recycle worker every 5000 requests (±200 jitter) to prevent memory leaks.
# Raised from 500 — 8 bots + frontend polling burned through 500 in ~4 minutes.
max_requests      = 5000
max_requests_jitter = 200
