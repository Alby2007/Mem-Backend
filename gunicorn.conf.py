# gunicorn.conf.py  — FastAPI (cutover complete)
bind         = "0.0.0.0:5050"
workers      = 1
worker_class = "uvicorn.workers.UvicornWorker"
wsgi_app     = "api_v2:app"
loglevel     = "info"
