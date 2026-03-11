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
# Auto-recycle worker to prevent memory leaks.
# At ~1 req/15s polling + 8 bots + ingest adapters, 50000 ≈ ~10 days before forced recycle.
# Bot threads restart after recycle via lifespan; keep high to minimise disruption.
max_requests      = 50000
max_requests_jitter = 1000
