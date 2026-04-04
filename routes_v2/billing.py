"""routes_v2/billing.py — Phase 4: Stripe billing endpoints.

Gate: checkout, portal, webhook all respond correctly against :8001.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()


class CheckoutRequest(BaseModel):
    tier: str
    annual: bool = False


@router.post("/stripe/checkout")
async def stripe_checkout(
    request: Request,
    data: CheckoutRequest,
    user_id: str = Depends(get_current_user),
):
    from middleware.stripe_billing import create_checkout_session

    if data.tier not in ("basic", "pro", "premium"):
        raise HTTPException(400, detail="invalid tier")

    try:
        user_row = ext.get_user(ext.DB_PATH, user_id)
        email = user_row.get("email") if user_row else None
    except Exception:
        email = None

    base_url    = str(request.base_url).rstrip("/")
    success_url = f"{base_url}/subscription?success=1"
    cancel_url  = f"{base_url}/subscription?cancelled=1"

    try:
        url = create_checkout_session(
            user_id=user_id,
            user_email=email or "",
            tier=data.tier,
            annual=data.annual,
            success_url=success_url,
            cancel_url=cancel_url,
        )
        return {"url": url}
    except RuntimeError as e:
        raise HTTPException(503, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/stripe/portal")
async def stripe_portal(
    request: Request,
    user_id: str = Depends(get_current_user),
):
    from middleware.stripe_billing import create_portal_session

    base_url   = str(request.base_url).rstrip("/")
    return_url = f"{base_url}/subscription"

    try:
        url = create_portal_session(user_id=user_id, return_url=return_url,
                                    db_path=ext.DB_PATH)
        return {"url": url}
    except ValueError as e:
        raise HTTPException(404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(503, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    from middleware.stripe_billing import handle_webhook

    payload    = await request.body()
    sig_header = request.headers.get("Stripe-Signature", "")

    try:
        result = handle_webhook(payload, sig_header, ext.DB_PATH)
        return result
    except ValueError:
        raise HTTPException(400, detail="invalid signature")
    except RuntimeError as e:
        raise HTTPException(503, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=str(e))
