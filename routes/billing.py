"""routes/billing.py — Stripe billing endpoints: checkout, portal, webhook."""

from __future__ import annotations

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('billing', __name__)


@bp.route('/stripe/checkout', methods=['POST'])
@ext.require_auth
def stripe_checkout():
    """
    POST /stripe/checkout
    Body: { "tier": "basic"|"pro"|"premium", "annual": false }
    Returns: { "url": "https://checkout.stripe.com/..." }
    """
    from middleware.stripe_billing import create_checkout_session

    data     = request.get_json(silent=True) or {}
    tier     = data.get('tier', '').lower()
    annual   = bool(data.get('annual', False))

    if tier not in ('basic', 'pro', 'premium'):
        return jsonify({'error': 'invalid tier'}), 400

    # Fetch user email for Stripe pre-fill
    try:
        user_row = ext.get_user(ext.DB_PATH, g.user_id)
        email    = user_row.get('email') if user_row else None
    except Exception:
        email = None

    base_url    = request.host_url.rstrip('/')
    success_url = f'{base_url}/subscription?success=1'
    cancel_url  = f'{base_url}/subscription?cancelled=1'

    try:
        url = create_checkout_session(
            user_id=g.user_id,
            user_email=email or '',
            tier=tier,
            annual=annual,
            success_url=success_url,
            cancel_url=cancel_url,
        )
        return jsonify({'url': url})
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/stripe/portal', methods=['POST'])
@ext.require_auth
def stripe_portal():
    """
    POST /stripe/portal
    Returns: { "url": "https://billing.stripe.com/..." }
    """
    from middleware.stripe_billing import create_portal_session

    base_url   = request.host_url.rstrip('/')
    return_url = f'{base_url}/subscription'

    try:
        url = create_portal_session(user_id=g.user_id, return_url=return_url)
        return jsonify({'url': url})
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/stripe/webhook', methods=['POST'])
def stripe_webhook():
    """
    POST /stripe/webhook
    Stripe sends signed events here. Verifies signature, updates user tier.
    """
    from middleware.stripe_billing import handle_webhook

    payload    = request.get_data()
    sig_header = request.headers.get('Stripe-Signature', '')

    try:
        result = handle_webhook(payload, sig_header, ext.DB_PATH)
        return jsonify(result)
    except ValueError:
        return jsonify({'error': 'invalid signature'}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500
