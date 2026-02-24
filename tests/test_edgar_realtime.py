"""
tests/test_edgar_realtime.py — Unit tests for ingest/edgar_realtime_adapter.py

Tests XML parsing, deduplication (in-memory + DB persistence on restart),
watchlist matching, atom emission, and queue writing.

No live SEC feed required — all tests use synthetic XML.
"""
import os
import sqlite3
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingest.edgar_realtime_adapter import (
    _parse_feed, _match_ticker, _ensure_seen_table, _load_seen_ids,
    _mark_seen, EDGARRealtimeAdapter,
)
from ingest.rss_adapter import _ensure_extraction_queue


# ── Synthetic feed XML ────────────────────────────────────────────────────────

def _make_feed(entries: list) -> str:
    """Build a minimal Atom feed XML with the given entries."""
    entry_xml = ''
    for e in entries:
        # & is invalid in both XML element content and attributes — escape to &amp;
        eid   = e['id'].replace('&', '&amp;')
        link  = e.get('link', 'https://www.sec.gov/filing/0001').replace('&', '&amp;')
        title = e['title'].replace('&', '&amp;')
        entry_xml += f"""
  <entry xmlns="http://www.w3.org/2005/Atom">
    <id>{eid}</id>
    <title>{title}</title>
    <updated>{e.get('updated', '2026-02-24T20:00:00Z')}</updated>
    <link href="{link}" rel="alternate"/>
  </entry>"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>SEC EDGAR Filings</title>
  <id>https://www.sec.gov/cgi-bin/browse-edgar</id>
  <updated>2026-02-24T20:00:00Z</updated>
{entry_xml}
</feed>"""


def _make_test_db() -> str:
    """Create a minimal temp DB with facts + extraction_queue tables."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            confidence REAL DEFAULT 0.5, source TEXT DEFAULT 'test',
            timestamp TEXT DEFAULT '2026-01-01T00:00:00+00:00',
            upsert INTEGER DEFAULT 0, metadata TEXT,
            confidence_effective REAL, hit_count INTEGER DEFAULT 0
        )
    """)
    _ensure_seen_table(conn)
    _ensure_extraction_queue(conn)
    conn.commit()
    conn.close()
    return path


# ── _parse_feed ───────────────────────────────────────────────────────────────

class TestParseFeed:
    def test_empty_feed(self):
        xml = _make_feed([])
        assert _parse_feed(xml) == []

    def test_single_entry(self):
        xml = _make_feed([{
            'id': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=8-K&dateb=&owner=include&count=1',
            'title': 'Apple Inc. (AAPL) 8-K',
            'link': 'https://www.sec.gov/filing/0001',
        }])
        entries = _parse_feed(xml)
        assert len(entries) == 1
        assert entries[0]['id'] != ''
        assert 'Apple' in entries[0]['company']

    def test_multiple_entries(self):
        xml = _make_feed([
            {'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'},
            {'id': 'id-002', 'title': 'Microsoft Corp. (MSFT) 8-K'},
            {'id': 'id-003', 'title': 'Some Unknown Corp (XYZ) 8-K'},
        ])
        entries = _parse_feed(xml)
        assert len(entries) == 3

    def test_company_name_extracted(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'}])
        entries = _parse_feed(xml)
        assert entries[0]['company'] == 'Apple Inc.'

    def test_title_no_paren(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc 8-K filing'}])
        entries = _parse_feed(xml)
        assert entries[0]['company'] == 'Apple Inc 8-K filing'

    def test_malformed_xml_returns_empty(self):
        assert _parse_feed('<broken xml') == []

    def test_empty_string_returns_empty(self):
        assert _parse_feed('') == []

    def test_link_extracted(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K',
                           'link': 'https://www.sec.gov/filing/123'}])
        entries = _parse_feed(xml)
        assert entries[0]['link'] == 'https://www.sec.gov/filing/123'

    def test_updated_extracted(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K',
                           'updated': '2026-02-24T18:30:00Z'}])
        entries = _parse_feed(xml)
        assert entries[0]['updated'] == '2026-02-24T18:30:00Z'


# ── _match_ticker ─────────────────────────────────────────────────────────────

class TestMatchTicker:
    def test_apple(self):
        assert _match_ticker('Apple Inc.') == 'AAPL'

    def test_microsoft(self):
        assert _match_ticker('Microsoft Corporation') == 'MSFT'

    def test_nvidia(self):
        assert _match_ticker('NVIDIA Corporation') == 'NVDA'

    def test_jpmorgan(self):
        assert _match_ticker('JPMorgan Chase & Co.') == 'JPM'

    def test_alphabet(self):
        assert _match_ticker('Alphabet Inc.') == 'GOOGL'

    def test_unknown_company(self):
        assert _match_ticker('Random Startup Corp.') is None

    def test_case_insensitive(self):
        assert _match_ticker('APPLE INC.') == 'AAPL'

    def test_partial_match(self):
        assert _match_ticker('Tesla Motors Inc') == 'TSLA'


# ── Deduplication DB helpers ──────────────────────────────────────────────────

class TestDeduplication:
    def setup_method(self):
        self.db_path = _make_test_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_load_empty(self):
        conn = sqlite3.connect(self.db_path)
        ids = _load_seen_ids(conn)
        conn.close()
        assert ids == set()

    def test_mark_and_load(self):
        conn = sqlite3.connect(self.db_path)
        _mark_seen(conn, 'id-001', 'Apple Inc.', 'AAPL')
        ids = _load_seen_ids(conn)
        conn.close()
        assert 'id-001' in ids

    def test_mark_multiple(self):
        conn = sqlite3.connect(self.db_path)
        _mark_seen(conn, 'id-001', 'Apple Inc.', 'AAPL')
        _mark_seen(conn, 'id-002', 'Microsoft Corp.', 'MSFT')
        ids = _load_seen_ids(conn)
        conn.close()
        assert len(ids) == 2

    def test_mark_duplicate_ignored(self):
        conn = sqlite3.connect(self.db_path)
        _mark_seen(conn, 'id-001', 'Apple Inc.', 'AAPL')
        _mark_seen(conn, 'id-001', 'Apple Inc.', 'AAPL')  # duplicate
        ids = _load_seen_ids(conn)
        conn.close()
        assert len(ids) == 1


# ── EDGARRealtimeAdapter init loads from DB ───────────────────────────────────

class TestAdapterInit:
    def setup_method(self):
        self.db_path = _make_test_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_seen_set_empty_on_fresh_db(self):
        adapter = EDGARRealtimeAdapter(db_path=self.db_path)
        assert len(adapter._seen_ids) == 0

    def test_seen_set_populated_from_db(self):
        # Pre-populate the DB with two seen IDs
        conn = sqlite3.connect(self.db_path)
        _mark_seen(conn, 'id-aaa', 'Apple Inc.', 'AAPL')
        _mark_seen(conn, 'id-bbb', 'Microsoft Corp.', 'MSFT')
        conn.close()

        # New adapter instance should load both from DB (simulates restart)
        adapter = EDGARRealtimeAdapter(db_path=self.db_path)
        assert 'id-aaa' in adapter._seen_ids
        assert 'id-bbb' in adapter._seen_ids
        assert len(adapter._seen_ids) == 2

    def test_restart_safe_no_duplicates(self):
        """Second adapter init sees same IDs as first — restart-safe dedup."""
        adapter1 = EDGARRealtimeAdapter(db_path=self.db_path)
        # Manually add to adapter1's seen set + DB
        conn = sqlite3.connect(self.db_path)
        _mark_seen(conn, 'id-xyz', 'Apple Inc.', 'AAPL')
        conn.close()

        adapter2 = EDGARRealtimeAdapter(db_path=self.db_path)
        assert 'id-xyz' in adapter2._seen_ids


# ── EDGARRealtimeAdapter.fetch() with synthetic feed ─────────────────────────

class TestAdapterFetch:
    def setup_method(self):
        self.db_path = _make_test_db()
        # Need a full facts table for the adapter to push atoms
        conn = sqlite3.connect(self.db_path)
        conn.execute("DROP TABLE IF EXISTS facts")
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                confidence REAL DEFAULT 0.5, source TEXT,
                timestamp TEXT DEFAULT '2026-01-01T00:00:00+00:00',
                upsert INTEGER DEFAULT 0, metadata TEXT,
                confidence_effective REAL, hit_count INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()

    def teardown_method(self):
        os.unlink(self.db_path)

    def _run_fetch_with_xml(self, xml: str) -> list:
        """Run adapter.fetch() with a mocked requests.get response."""
        import unittest.mock as mock
        adapter = EDGARRealtimeAdapter(db_path=self.db_path)

        mock_resp = mock.MagicMock()
        mock_resp.text = xml
        mock_resp.raise_for_status = mock.MagicMock()

        with mock.patch('ingest.edgar_realtime_adapter.requests.get',
                        return_value=mock_resp):
            return adapter.fetch()

    def test_known_ticker_emits_atom(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'}])
        atoms = self._run_fetch_with_xml(xml)
        assert len(atoms) == 1
        assert atoms[0].subject == 'aapl'
        assert atoms[0].predicate == 'catalyst'
        assert '8-K' in atoms[0].object

    def test_unknown_company_no_atom(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Unknown Corp XYZ 8-K'}])
        atoms = self._run_fetch_with_xml(xml)
        assert len(atoms) == 0

    def test_dedup_skips_seen(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'}])
        adapter = EDGARRealtimeAdapter(db_path=self.db_path)
        adapter._seen_ids.add('id-001')  # Mark as already seen

        import unittest.mock as mock
        mock_resp = mock.MagicMock()
        mock_resp.text = xml
        mock_resp.raise_for_status = mock.MagicMock()
        with mock.patch('ingest.edgar_realtime_adapter.requests.get',
                        return_value=mock_resp):
            atoms = adapter.fetch()
        assert len(atoms) == 0

    def test_second_run_deduplicates(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'}])
        atoms1 = self._run_fetch_with_xml(xml)
        assert len(atoms1) == 1

        # Same XML again — id-001 now in seen set
        atoms2 = self._run_fetch_with_xml(xml)
        assert len(atoms2) == 0

    def test_multiple_known_tickers(self):
        xml = _make_feed([
            {'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'},
            {'id': 'id-002', 'title': 'NVIDIA Corporation (NVDA) 8-K'},
            {'id': 'id-003', 'title': 'Unknown Corp 8-K'},
        ])
        atoms = self._run_fetch_with_xml(xml)
        assert len(atoms) == 2
        subjects = {a.subject for a in atoms}
        assert 'aapl' in subjects
        assert 'nvda' in subjects

    def test_atom_source(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'}])
        atoms = self._run_fetch_with_xml(xml)
        assert atoms[0].source == 'regulatory_filing_sec_realtime'

    def test_atom_confidence(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K'}])
        atoms = self._run_fetch_with_xml(xml)
        assert atoms[0].confidence == 0.90

    def test_network_error_returns_empty(self):
        import unittest.mock as mock
        import requests
        adapter = EDGARRealtimeAdapter(db_path=self.db_path)
        with mock.patch('ingest.edgar_realtime_adapter.requests.get',
                        side_effect=requests.ConnectionError('offline')):
            atoms = adapter.fetch()
        assert atoms == []

    def test_queue_entry_written(self):
        xml = _make_feed([{'id': 'id-001', 'title': 'Apple Inc. (AAPL) 8-K',
                           'link': 'https://sec.gov/filing/0001'}])
        self._run_fetch_with_xml(xml)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM extraction_queue WHERE source='regulatory_filing_sec_realtime'")
        count = c.fetchone()[0]
        conn.close()
        assert count == 1
