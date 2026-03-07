"""
routes/__init__.py — Blueprint registration.

All route modules are imported and registered here.
"""

from __future__ import annotations


def register_blueprints(app):
    """Register all route Blueprints on the Flask app."""
    from routes.health import bp as health_bp
    from routes.kb import bp as kb_bp
    from routes.ingest_routes import bp as ingest_bp
    from routes.chat import bp as chat_bp
    from routes.markets import bp as markets_bp
    from routes.analytics_ import bp as analytics_bp
    from routes.users import bp as users_bp
    from routes.auth import bp as auth_bp
    from routes.billing import bp as billing_bp
    from routes.paper import bp as paper_bp
    from routes.patterns import bp as patterns_bp
    from routes.telegram import bp as telegram_bp
    from routes.network import bp as network_bp
    from routes.thesis import bp as thesis_bp
    from routes.waitlist import bp as waitlist_bp

    for blueprint in (
        health_bp, kb_bp, ingest_bp, chat_bp, markets_bp,
        analytics_bp, users_bp, auth_bp, billing_bp, paper_bp,
        patterns_bp, telegram_bp, network_bp, thesis_bp, waitlist_bp,
    ):
        app.register_blueprint(blueprint)
