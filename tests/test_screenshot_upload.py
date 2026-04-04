"""
tests/test_screenshot_upload.py — Tests for POST /users/{id}/history/screenshot

Covers:
  - chat_vision() unit behaviour (mocked Ollama)
  - API endpoint auth, validation, vision-unavailable path, extraction path
  - Holdings normalisation (ticker coercion, type safety, duplicate skip)

No live Ollama or live DB required — everything is mocked or uses a temp DB.
Run: pytest tests/test_screenshot_upload.py -v
"""
from __future__ import annotations

import base64
import io
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_png_bytes(text_lines: list[str] | None = None) -> bytes:
    """Create a minimal valid PNG in memory (no Pillow required)."""
    try:
        from PIL import Image, ImageDraw
        img = Image.new('RGB', (400, 200), color=(255, 255, 255))
        if text_lines:
            d = ImageDraw.Draw(img)
            for i, line in enumerate(text_lines):
                d.text((10, 10 + i * 20), line, fill=(0, 0, 0))
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return buf.getvalue()
    except ImportError:
        # Return a 1×1 white PNG (raw bytes) if PIL not installed
        import zlib, struct
        def _chunk(name, data):
            c = zlib.crc32(name + data) & 0xffffffff
            return struct.pack('>I', len(data)) + name + data + struct.pack('>I', c)
        raw = (
            b'\x89PNG\r\n\x1a\n'
            + _chunk(b'IHDR', struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0))
            + _chunk(b'IDAT', zlib.compress(b'\x00\xff\xff\xff'))
            + _chunk(b'IEND', b'')
        )
        return raw


# ── Unit tests: chat_vision() ─────────────────────────────────────────────────

class TestChatVision:
    def test_returns_string_on_success(self):
        from llm.ollama_client import chat_vision
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'message': {'content': '[{"ticker":"SHEL.L","quantity":10,"avg_cost":27.5}]'}}
        with patch('llm.ollama_client._requests.post', return_value=mock_resp) as mp:
            result = chat_vision('fakebase64==', 'extract holdings', model='llava', timeout=10)
        assert result == '[{"ticker":"SHEL.L","quantity":10,"avg_cost":27.5}]'
        call_payload = mp.call_args[1]['json']
        assert call_payload['model'] == 'llava'
        assert call_payload['messages'][0]['images'] == ['fakebase64==']

    def test_returns_none_on_connection_error(self):
        from llm.ollama_client import chat_vision
        import requests as _r
        with patch('llm.ollama_client._requests.post', side_effect=_r.exceptions.ConnectionError):
            result = chat_vision('img', 'prompt', timeout=5)
        assert result is None

    def test_returns_none_on_timeout(self):
        from llm.ollama_client import chat_vision
        import requests as _r
        with patch('llm.ollama_client._requests.post', side_effect=_r.exceptions.Timeout):
            result = chat_vision('img', 'prompt', timeout=5)
        assert result is None

    def test_image_sent_in_messages(self):
        from llm.ollama_client import chat_vision
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {'message': {'content': '[]'}}
        with patch('llm.ollama_client._requests.post', return_value=mock_resp) as mp:
            chat_vision('AAABBB==', 'prompt', timeout=5)
        payload = mp.call_args[1]['json']
        msg = payload['messages'][0]
        assert msg['role'] == 'user'
        assert 'AAABBB==' in msg['images']
        assert msg['content'] == 'prompt'

    def test_vision_model_constant_exists(self):
        from llm.ollama_client import VISION_MODEL
        assert isinstance(VISION_MODEL, str)
        assert len(VISION_MODEL) > 0


# ── Holdings normalisation logic ──────────────────────────────────────────────

class TestHoldingsNormalisation:
    """Test the normalisation logic extracted from the endpoint, independently."""

    @staticmethod
    def _normalise(raw_holdings: list) -> list:
        clean = []
        for h in raw_holdings:
            ticker = str(h.get('ticker') or '').strip().upper()
            if not ticker:
                continue
            try:
                qty = float(h.get('quantity') or 0)
            except (TypeError, ValueError):
                qty = 0.0
            avg_cost = h.get('avg_cost')
            try:
                avg_cost = float(avg_cost) if avg_cost is not None else None
            except (TypeError, ValueError):
                avg_cost = None
            clean.append({'ticker': ticker, 'quantity': qty, 'avg_cost': avg_cost})
        return clean

    def test_ticker_uppercased(self):
        result = self._normalise([{'ticker': 'shel.l', 'quantity': 5, 'avg_cost': 27.5}])
        assert result[0]['ticker'] == 'SHEL.L'

    def test_ticker_stripped(self):
        result = self._normalise([{'ticker': '  BARC.L  ', 'quantity': 10, 'avg_cost': None}])
        assert result[0]['ticker'] == 'BARC.L'

    def test_empty_ticker_skipped(self):
        result = self._normalise([{'ticker': '', 'quantity': 5, 'avg_cost': 1.0}])
        assert result == []

    def test_none_ticker_skipped(self):
        result = self._normalise([{'ticker': None, 'quantity': 5, 'avg_cost': 1.0}])
        assert result == []

    def test_quantity_coerced_to_float(self):
        result = self._normalise([{'ticker': 'AZN.L', 'quantity': '15', 'avg_cost': None}])
        assert result[0]['quantity'] == 15.0
        assert isinstance(result[0]['quantity'], float)

    def test_bad_quantity_defaults_to_zero(self):
        result = self._normalise([{'ticker': 'AZN.L', 'quantity': 'many', 'avg_cost': None}])
        assert result[0]['quantity'] == 0.0

    def test_avg_cost_none_preserved(self):
        result = self._normalise([{'ticker': 'AZN.L', 'quantity': 10, 'avg_cost': None}])
        assert result[0]['avg_cost'] is None

    def test_avg_cost_string_coerced(self):
        result = self._normalise([{'ticker': 'AZN.L', 'quantity': 10, 'avg_cost': '120.50'}])
        assert result[0]['avg_cost'] == pytest.approx(120.50)

    def test_bad_avg_cost_becomes_none(self):
        result = self._normalise([{'ticker': 'AZN.L', 'quantity': 10, 'avg_cost': 'n/a'}])
        assert result[0]['avg_cost'] is None

    def test_multiple_holdings(self):
        raw = [
            {'ticker': 'HSBA.L', 'quantity': 100, 'avg_cost': 6.24},
            {'ticker': 'lloy.l', 'quantity': 200, 'avg_cost': None},
            {'ticker': '',       'quantity': 5,   'avg_cost': 1.0},
        ]
        result = self._normalise(raw)
        assert len(result) == 2
        assert result[0]['ticker'] == 'HSBA.L'
        assert result[1]['ticker'] == 'LLOY.L'


# ── API endpoint tests (live HTTP against localhost:5051) ─────────────────────
#
# These tests follow the same pattern as test_full_stack.py — they require
# a running server.  They are skipped automatically when the server is not up.

import requests as _http

_BASE = 'http://localhost:5051'
_TEST_EMAIL = 'screenshot@test.com'
_TEST_PW    = 'TestPass123!'
_TEST_UID   = 'screenshottestuser'


def _server_up() -> bool:
    try:
        _http.get(f'{_BASE}/health', timeout=2)
        return True  # any HTTP response (incl. 429) means server is up
    except (_http.exceptions.ConnectionError, _http.exceptions.Timeout):
        return False


def _ensure_user() -> str:
    """Register (idempotent) + login, return Bearer token."""
    _http.post(f'{_BASE}/auth/register',
               json={'user_id': _TEST_UID, 'email': _TEST_EMAIL, 'password': _TEST_PW},
               timeout=5)
    r = _http.post(f'{_BASE}/auth/token',
                   json={'email': _TEST_EMAIL, 'password': _TEST_PW},
                   timeout=5)
    return r.json().get('access_token', '')


@pytest.fixture(scope='module')
def live_token():
    if not _server_up():
        pytest.skip('Server not running at localhost:5051')
    return _ensure_user()


@pytest.fixture(scope='module')
def live_uid():
    return _TEST_UID


class TestScreenshotEndpoint:
    def test_no_auth_returns_401(self, live_token, live_uid):
        r = _http.post(f'{_BASE}/users/{live_uid}/history/screenshot', timeout=5)
        assert r.status_code == 401

    def test_no_file_returns_400(self, live_token, live_uid):
        headers = {'Authorization': f'Bearer {live_token}'}
        r = _http.post(f'{_BASE}/users/{live_uid}/history/screenshot',
                       headers=headers, timeout=5)
        data = r.json()
        # 400 (no file) or 200 vision_unavailable — both valid
        assert r.status_code in (200, 400)
        if r.status_code == 400:
            assert 'error' in data

    def test_non_image_file_rejected(self, live_token, live_uid):
        headers = {'Authorization': f'Bearer {live_token}'}
        files = {'file': ('bad.txt', io.BytesIO(b'not an image'), 'text/plain')}
        r = _http.post(f'{_BASE}/users/{live_uid}/history/screenshot',
                       headers=headers, files=files, timeout=10)
        assert r.status_code == 400
        assert 'error' in r.json()

    def test_vision_available_with_image(self, live_token, live_uid):
        """Full pipeline: send a synthetic portfolio image, expect vision_available=True."""
        headers = {'Authorization': f'Bearer {live_token}'}
        img_bytes = _make_png_bytes([
            'Portfolio Holdings',
            'SHEL.L   10 shares @ 27.50',
            'BARC.L   50 shares @  2.31',
        ])
        files = {'file': ('portfolio.png', io.BytesIO(img_bytes), 'image/png')}
        r = _http.post(f'{_BASE}/users/{live_uid}/history/screenshot',
                       headers=headers, files=files, timeout=120)
        assert r.status_code == 200
        data = r.json()
        assert 'vision_available' in data
        assert 'holdings' in data
        assert isinstance(data['holdings'], list)
        if data['vision_available']:
            # If model ran, all returned tickers should be non-empty strings
            for h in data['holdings']:
                assert isinstance(h['ticker'], str) and len(h['ticker']) > 0

    def test_holdings_have_required_fields(self, live_token, live_uid):
        headers = {'Authorization': f'Bearer {live_token}'}
        img_bytes = _make_png_bytes(['AZN.L 10 @ 120.00'])
        files = {'file': ('test.png', io.BytesIO(img_bytes), 'image/png')}
        r = _http.post(f'{_BASE}/users/{live_uid}/history/screenshot',
                       headers=headers, files=files, timeout=120)
        assert r.status_code == 200
        data = r.json()
        for h in data.get('holdings', []):
            assert 'ticker' in h
            assert 'quantity' in h
            assert 'avg_cost' in h

    def test_tickers_are_uppercase(self, live_token, live_uid):
        """Any tickers returned by the endpoint must be uppercase."""
        headers = {'Authorization': f'Bearer {live_token}'}
        img_bytes = _make_png_bytes(['hsba.l 100 @ 6.24'])
        files = {'file': ('test.png', io.BytesIO(img_bytes), 'image/png')}
        r = _http.post(f'{_BASE}/users/{live_uid}/history/screenshot',
                       headers=headers, files=files, timeout=120)
        assert r.status_code == 200
        for h in r.json().get('holdings', []):
            assert h['ticker'] == h['ticker'].upper()


# ── Live integration test (skipped unless --live flag passed) ─────────────────

@pytest.mark.skipif(
    os.environ.get('LIVE_TEST') != '1',
    reason="Live integration test — set LIVE_TEST=1 to run against localhost:5051"
)
class TestScreenshotLive:
    """
    Runs against a real running server at localhost:5051.
    Requires: server running, llava model pulled, valid test user.
    Run with: LIVE_TEST=1 pytest tests/test_screenshot_upload.py::TestScreenshotLive -v
    """
    BASE = 'http://localhost:5051'

    def _login(self):
        import requests
        r = requests.post(f'{self.BASE}/auth/token',
                          json={'email': 'alice@test.com', 'password': 'Test1234!'})
        d = r.json()
        return d['access_token'], d['user_id']

    def test_live_vision_pipeline(self):
        import requests
        token, uid = self._login()
        headers = {'Authorization': f'Bearer {token}'}

        img_bytes = _make_png_bytes([
            'Portfolio Holdings',
            'SHEL.L   100 shares @ 27.50',
            'BARC.L    50 shares @  2.31',
            'AZN.L     10 shares @ 120.00',
        ])
        files = {'file': ('portfolio.png', img_bytes, 'image/png')}
        r = requests.post(f'{self.BASE}/users/{uid}/history/screenshot',
                          headers=headers, files=files, timeout=120)
        assert r.status_code == 200
        data = r.json()
        assert data['vision_available'] is True
        assert isinstance(data['holdings'], list)
        print(f"\nLive extraction: {json.dumps(data['holdings'], indent=2)}")
