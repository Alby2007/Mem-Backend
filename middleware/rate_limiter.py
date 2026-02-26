"""
middleware/rate_limiter.py — Flask-Limiter configuration

Provides a pre-configured Limiter instance and per-class limit strings.
Import `limiter` and call limiter.init_app(app) in api.py.

If flask-limiter is not installed the module degrades gracefully — `limiter`
is None and the `limit` decorator is a no-op passthrough.

USAGE
=====
    from middleware.rate_limiter import limiter, rate_limit

    limiter.init_app(app)   # in api.py startup

    @app.route('/expensive')
    @rate_limit('snapshot')
    @require_auth
    def expensive():
        ...
"""

from __future__ import annotations

import logging
from functools import wraps

_log = logging.getLogger(__name__)

# Per-class rate limit strings
RATE_LIMITS: dict[str, str] = {
    'auth':      '10 per minute',
    'chat':      '30 per hour',
    'snapshot':  '5 per hour',
    'patterns':  '60 per hour',
    'portfolio': '20 per hour',
    'write':     '30 per hour',
    'default':   '200 per day',
}

try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address

    limiter = Limiter(
        key_func=get_remote_address,
        default_limits=[RATE_LIMITS['default'], '200 per hour'],
        storage_uri='memory://',
    )

    def rate_limit(cls: str):
        """Apply a named rate-limit class to a route."""
        limit_str = RATE_LIMITS.get(cls, RATE_LIMITS['default'])
        return limiter.limit(limit_str)

    HAS_LIMITER = True

except ImportError:
    _log.warning(
        'flask-limiter not installed — rate limiting disabled. '
        'Install with: pip install flask-limiter'
    )

    class _NoOpLimiter:
        def init_app(self, app):
            pass
        def limit(self, *a, **kw):
            def decorator(f):
                return f
            return decorator

    limiter = _NoOpLimiter()      # type: ignore

    def rate_limit(cls: str):     # type: ignore
        """No-op when flask-limiter is not installed."""
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                return f(*args, **kwargs)
            return wrapper
        return decorator

    HAS_LIMITER = False
