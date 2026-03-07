"""
tests/test_session_manager.py — Unit tests for services.session.SessionManager.

Covers thread safety, TTL eviction, and API correctness.
"""

from __future__ import annotations

import threading
import time
import unittest

from services.session import SessionManager


class TestSessionManagerStreaks(unittest.TestCase):
    """Test streak get/set/reset/has/all operations."""

    def setUp(self):
        self.sm = SessionManager(ttl_sec=60)

    def test_get_streak_default(self):
        s = self.sm.get_streak('new_session')
        self.assertEqual(s, {'streak': 0, 'last_stress': 0.0})

    def test_set_and_get_streak(self):
        self.sm.set_streak('s1', {'streak': 3, 'last_stress': 0.75})
        s = self.sm.get_streak('s1')
        self.assertEqual(s['streak'], 3)
        self.assertAlmostEqual(s['last_stress'], 0.75)

    def test_reset_streak(self):
        self.sm.set_streak('s1', {'streak': 5, 'last_stress': 0.9})
        self.sm.reset_streak('s1')
        s = self.sm.get_streak('s1')
        self.assertEqual(s['streak'], 0)
        self.assertAlmostEqual(s['last_stress'], 0.0)

    def test_has_streak(self):
        self.assertFalse(self.sm.has_streak('nonexistent'))
        self.sm.get_streak('s1')  # creates entry
        self.assertTrue(self.sm.has_streak('s1'))

    def test_all_streaks(self):
        self.sm.set_streak('a', {'streak': 1, 'last_stress': 0.1})
        self.sm.set_streak('b', {'streak': 2, 'last_stress': 0.2})
        result = self.sm.all_streaks()
        self.assertIn('a', result)
        self.assertIn('b', result)
        self.assertEqual(result['a']['streak'], 1)

    def test_active_and_total_count(self):
        self.sm.set_streak('a', {'streak': 0, 'last_stress': 0.0})
        self.sm.set_streak('b', {'streak': 3, 'last_stress': 0.5})
        self.sm.set_streak('c', {'streak': 1, 'last_stress': 0.2})
        self.assertEqual(self.sm.total_streak_count(), 3)
        self.assertEqual(self.sm.active_streak_count(), 2)

    def test_get_streak_returns_copy(self):
        """Mutating the returned dict should NOT affect internal state."""
        s = self.sm.get_streak('s1')
        s['streak'] = 999
        s2 = self.sm.get_streak('s1')
        self.assertEqual(s2['streak'], 0)


class TestSessionManagerTickers(unittest.TestCase):
    """Test ticker get/set/has/pop operations."""

    def setUp(self):
        self.sm = SessionManager(ttl_sec=60)

    def test_get_tickers_none_by_default(self):
        self.assertIsNone(self.sm.get_tickers('new'))

    def test_set_and_get_tickers(self):
        self.sm.set_tickers('s1', ['AAPL', 'MSFT'])
        result = self.sm.get_tickers('s1')
        self.assertEqual(result, ['AAPL', 'MSFT'])

    def test_has_tickers(self):
        self.assertFalse(self.sm.has_tickers('s1'))
        self.sm.set_tickers('s1', ['TSLA'])
        self.assertTrue(self.sm.has_tickers('s1'))

    def test_pop_tickers(self):
        self.sm.set_tickers('s1', ['AAPL'])
        popped = self.sm.pop_tickers('s1')
        self.assertEqual(popped, ['AAPL'])
        self.assertIsNone(self.sm.get_tickers('s1'))

    def test_pop_tickers_nonexistent(self):
        self.assertIsNone(self.sm.pop_tickers('nonexistent'))

    def test_get_tickers_returns_copy(self):
        self.sm.set_tickers('s1', ['AAPL'])
        t = self.sm.get_tickers('s1')
        t.append('MSFT')
        self.assertEqual(self.sm.get_tickers('s1'), ['AAPL'])


class TestSessionManagerPortfolioTickers(unittest.TestCase):
    """Test portfolio ticker get/set/pop operations."""

    def setUp(self):
        self.sm = SessionManager(ttl_sec=60)

    def test_get_portfolio_tickers_none_by_default(self):
        self.assertIsNone(self.sm.get_portfolio_tickers('new'))

    def test_set_and_get_portfolio_tickers(self):
        self.sm.set_portfolio_tickers('s1', ['GOOG', 'AMZN'])
        result = self.sm.get_portfolio_tickers('s1')
        self.assertEqual(result, ['GOOG', 'AMZN'])

    def test_pop_portfolio_tickers(self):
        self.sm.set_portfolio_tickers('s1', ['META'])
        popped = self.sm.pop_portfolio_tickers('s1')
        self.assertEqual(popped, ['META'])
        self.assertIsNone(self.sm.get_portfolio_tickers('s1'))


class TestSessionManagerClearSession(unittest.TestCase):
    """Test bulk clear."""

    def setUp(self):
        self.sm = SessionManager(ttl_sec=60)

    def test_clear_session_removes_all(self):
        self.sm.set_streak('s1', {'streak': 5, 'last_stress': 0.8})
        self.sm.set_tickers('s1', ['AAPL'])
        self.sm.set_portfolio_tickers('s1', ['MSFT'])
        self.sm.clear_session('s1')
        self.assertFalse(self.sm.has_streak('s1'))
        self.assertIsNone(self.sm.get_tickers('s1'))
        self.assertIsNone(self.sm.get_portfolio_tickers('s1'))

    def test_clear_nonexistent_session_no_error(self):
        self.sm.clear_session('nonexistent')  # should not raise


class TestSessionManagerTTL(unittest.TestCase):
    """Test TTL-based eviction."""

    def test_expired_sessions_evicted(self):
        sm = SessionManager(ttl_sec=0)  # instant expiry
        sm.set_streak('s1', {'streak': 3, 'last_stress': 0.5})
        sm.set_tickers('s1', ['AAPL'])
        sm.set_portfolio_tickers('s1', ['MSFT'])
        # Force cleanup
        sm._last_cleanup = 0  # ensure cleanup runs
        sm._cleanup()
        self.assertFalse(sm.has_streak('s1'))
        self.assertIsNone(sm.get_tickers('s1'))
        self.assertIsNone(sm.get_portfolio_tickers('s1'))

    def test_fresh_sessions_survive_cleanup(self):
        sm = SessionManager(ttl_sec=3600)
        sm.set_streak('s1', {'streak': 2, 'last_stress': 0.3})
        sm._cleanup()
        self.assertTrue(sm.has_streak('s1'))


class TestSessionManagerConcurrency(unittest.TestCase):
    """Test thread safety under concurrent access."""

    def test_concurrent_streak_writes(self):
        sm = SessionManager(ttl_sec=3600)
        errors = []

        def writer(tid):
            try:
                for i in range(100):
                    sid = f'session_{tid}'
                    s = sm.get_streak(sid)
                    s['streak'] = s['streak'] + 1
                    sm.set_streak(sid, s)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        # Each of 10 threads wrote 100 increments to its own session
        for tid in range(10):
            s = sm.get_streak(f'session_{tid}')
            self.assertEqual(s['streak'], 100)

    def test_concurrent_mixed_operations(self):
        sm = SessionManager(ttl_sec=3600)
        errors = []

        def worker(tid):
            try:
                sid = f'session_{tid}'
                for i in range(50):
                    sm.set_tickers(sid, [f'T{i}'])
                    sm.set_portfolio_tickers(sid, [f'P{i}'])
                    sm.get_tickers(sid)
                    sm.get_portfolio_tickers(sid)
                    s = sm.get_streak(sid)
                    s['streak'] += 1
                    sm.set_streak(sid, s)
                    if i % 10 == 0:
                        sm.all_streaks()
                        sm.active_streak_count()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])

    def test_concurrent_same_session_no_crash(self):
        """Multiple threads hitting the SAME session_id should not crash."""
        sm = SessionManager(ttl_sec=3600)
        errors = []

        def worker(_):
            try:
                for i in range(100):
                    sm.get_streak('shared')
                    sm.set_streak('shared', {'streak': i, 'last_stress': float(i)})
                    sm.set_tickers('shared', [f'T{i}'])
                    sm.get_tickers('shared')
                    sm.pop_tickers('shared')
                    sm.set_portfolio_tickers('shared', [f'P{i}'])
                    sm.pop_portfolio_tickers('shared')
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()
