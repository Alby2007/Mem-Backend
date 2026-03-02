"""
middleware/stripe_billing.py — Stripe Checkout + webhook handling.

Environment variables required:
  STRIPE_SECRET_KEY        — sk_live_... or sk_test_...
  STRIPE_WEBHOOK_SECRET    — whsec_... (from Stripe dashboard → Webhooks)
  STRIPE_PRICE_BASIC_M     — price_... monthly Basic
  STRIPE_PRICE_BASIC_A     — price_... annual  Basic
  STRIPE_PRICE_PRO_M       — price_... monthly Pro
  STRIPE_PRICE_PRO_A       — price_... annual  Pro
  STRIPE_PRICE_PREMIUM_M   — price_... monthly Premium
  STRIPE_PRICE_PREMIUM_A   — price_... annual  Premium

Tier mapping is stored in Stripe product metadata:
  tier = basic | pro | premium
"""

from __future__ import annotations

import os
import logging

import stripe

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _sk() -> str:
    key = os.environ.get('STRIPE_SECRET_KEY', '')
    if not key:
        raise RuntimeError('STRIPE_SECRET_KEY not configured')
    return key


def _price_id(tier: str, annual: bool) -> str:
    suffix = 'A' if annual else 'M'
    env_var = f'STRIPE_PRICE_{tier.upper()}_{suffix}'
    price = os.environ.get(env_var, '')
    if not price:
        raise RuntimeError(f'{env_var} not configured')
    return price


# ---------------------------------------------------------------------------
# Checkout session
# ---------------------------------------------------------------------------

def create_checkout_session(
    user_id: str,
    user_email: str,
    tier: str,
    annual: bool,
    success_url: str,
    cancel_url: str,
) -> str:
    """
    Create a Stripe Checkout Session and return its URL.
    The session includes user_id in metadata so the webhook can match it.
    """
    stripe.api_key = _sk()
    price_id = _price_id(tier, annual)

    params = dict(
        mode='subscription',
        line_items=[{'price': price_id, 'quantity': 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={'user_id': user_id, 'tier': tier},
        subscription_data={'metadata': {'user_id': user_id, 'tier': tier}},
    )
    if user_email and '@' in user_email:
        params['customer_email'] = user_email

    session = stripe.checkout.Session.create(**params)
    return session.url


# ---------------------------------------------------------------------------
# Customer portal
# ---------------------------------------------------------------------------

def create_portal_session(user_id: str, return_url: str) -> str:
    """
    Create a Stripe Customer Portal session for an existing customer.
    Looks up the customer by metadata user_id.
    """
    stripe.api_key = _sk()

    customers = stripe.Customer.search(query=f'metadata["user_id"]:"{user_id}"')
    if not customers.data:
        raise ValueError(f'No Stripe customer found for user_id={user_id}')

    customer_id = customers.data[0].id
    portal = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=return_url,
    )
    return portal.url


# ---------------------------------------------------------------------------
# Webhook handler
# ---------------------------------------------------------------------------

def _tier_from_event(event_object) -> str | None:
    """Extract tier from subscription or checkout session metadata."""
    meta = getattr(event_object, 'metadata', {}) or {}
    return meta.get('tier')


def _user_id_from_event(event_object) -> str | None:
    meta = getattr(event_object, 'metadata', {}) or {}
    return meta.get('user_id')


def handle_webhook(payload: bytes, sig_header: str, db_path: str) -> dict:
    """
    Verify and process a Stripe webhook event.
    Returns {'status': 'ok'} or raises on error.
    """
    secret = os.environ.get('STRIPE_WEBHOOK_SECRET', '')
    if not secret:
        raise RuntimeError('STRIPE_WEBHOOK_SECRET not configured')

    stripe.api_key = _sk()

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, secret)
    except stripe.error.SignatureVerificationError as exc:
        log.warning('Stripe webhook signature invalid: %s', exc)
        raise ValueError('Invalid signature') from exc

    etype = event['type']
    obj   = event['data']['object']

    log.info('Stripe webhook received: %s', etype)

    if etype == 'checkout.session.completed':
        _handle_checkout_completed(obj, db_path)

    elif etype in ('customer.subscription.updated', 'customer.subscription.created'):
        _handle_subscription_active(obj, db_path)

    elif etype == 'customer.subscription.deleted':
        _handle_subscription_cancelled(obj, db_path)

    elif etype in ('invoice.payment_failed', 'invoice.payment_action_required'):
        _handle_payment_failed(obj, db_path)

    return {'status': 'ok'}


def _set_user_tier(db_path: str, user_id: str, tier: str) -> None:
    from users.user_store import set_user_tier  # lazy to avoid circular imports
    set_user_tier(db_path, user_id, tier)
    log.info('Set tier=%s for user_id=%s', tier, user_id)


def _handle_checkout_completed(session, db_path: str) -> None:
    user_id = _user_id_from_event(session)
    tier    = _tier_from_event(session)
    if user_id and tier:
        _set_user_tier(db_path, user_id, tier)


def _handle_subscription_active(subscription, db_path: str) -> None:
    user_id = _user_id_from_event(subscription)
    tier    = _tier_from_event(subscription)
    status  = getattr(subscription, 'status', None) or subscription.get('status')

    if not user_id:
        log.warning('Subscription event missing user_id metadata')
        return

    if status in ('active', 'trialing') and tier:
        _set_user_tier(db_path, user_id, tier)
    elif status in ('past_due', 'unpaid', 'canceled', 'paused'):
        _set_user_tier(db_path, user_id, 'free')


def _handle_subscription_cancelled(subscription, db_path: str) -> None:
    user_id = _user_id_from_event(subscription)
    if user_id:
        _set_user_tier(db_path, user_id, 'free')


def _handle_payment_failed(invoice, db_path: str) -> None:
    # Don't immediately downgrade on first failure — Stripe retries.
    # Log only; subscription.updated with status=past_due will fire separately.
    sub_id = getattr(invoice, 'subscription', None) or invoice.get('subscription')
    log.warning('Payment failed for subscription %s', sub_id)
