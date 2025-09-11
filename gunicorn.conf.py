# gunicorn.conf.py
bind = "0.0.0.0:8080"
workers = 1
worker_class = "sync"
timeout = 120  # Increase worker timeout to 2 minutes
keepalive = 5
max_requests = 1000
max_requests_jitter = 100
preload_app = True