from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from common.btc5m_dataset_db import resolve_repo_path
from common.btc5m_reference_feed import ReferenceOhlcvAggregator
from common.btc5m_trade_tick_feed import normalize_trade_row
from common.config_hash import stable_config_hash


class StableConfigHashTests(unittest.TestCase):
    def test_hash_is_stable_across_key_order(self) -> None:
        left = stable_config_hash({"name": "collector", "interval_sec": 10})
        right = stable_config_hash({"interval_sec": 10, "name": "collector"})

        self.assertEqual(left, right)

    def test_hash_changes_when_payload_changes(self) -> None:
        base = stable_config_hash({"name": "collector", "interval_sec": 10})
        changed = stable_config_hash({"name": "collector", "interval_sec": 15})

        self.assertNotEqual(base, changed)

    def test_path_values_are_serialized_consistently(self) -> None:
        path = Path("runtime") / "data.db"

        self.assertEqual(
            stable_config_hash({"path": path}),
            stable_config_hash({"path": str(path)}),
        )


class RepoPathTests(unittest.TestCase):
    def test_relative_values_resolve_under_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)

            path = resolve_repo_path(
                "runtime/logs/test.log",
                default_path=root_dir / "fallback.log",
                root_dir=root_dir,
            )

        self.assertEqual(path, root_dir / "runtime" / "logs" / "test.log")

    def test_blank_values_use_default_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)

            path = resolve_repo_path(
                "",
                default_path=Path("runtime/default.log"),
                root_dir=root_dir,
            )

        self.assertEqual(path, root_dir / "runtime" / "default.log")


class FeedNormalizationTests(unittest.TestCase):
    def test_reference_aggregator_rolls_completed_candle(self) -> None:
        aggregator = ReferenceOhlcvAggregator(source_name="source", symbol="btcusdt")

        self.assertIsNone(aggregator.update({"ts_utc": 0, "btc_price": 10.0, "volume_1s": 1.5}))
        self.assertIsNone(aggregator.update({"ts_utc": 59, "btc_price": 12.0, "volume_1s": 2.5}))
        candle = aggregator.update({"ts_utc": 60, "btc_price": 11.0, "volume_1s": 1.0})

        self.assertIsNotNone(candle)
        assert candle is not None
        self.assertEqual(candle["candle_ts"], 0)
        self.assertEqual(candle["symbol"], "BTCUSDT")
        self.assertEqual(candle["open"], 10.0)
        self.assertEqual(candle["high"], 12.0)
        self.assertEqual(candle["low"], 10.0)
        self.assertEqual(candle["close"], 12.0)
        self.assertEqual(candle["volume"], 4.0)
        self.assertEqual(candle["trade_count"], 2)

    def test_trade_row_normalization_accepts_valid_yes_trade(self) -> None:
        normalized = normalize_trade_row(
            {
                "asset": "yes-token",
                "side": "buy",
                "transactionHash": "0xabc",
                "timestamp": "1710000000",
                "price": "0.25",
                "size": "100",
                "proxyWallet": "0xwallet",
            },
            market_id="market-1",
            market_slug="btc-up-down",
            yes_token_id="yes-token",
            no_token_id="no-token",
            collected_ts=1710000001,
        )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["outcome"], "YES")
        self.assertEqual(normalized["side"], "BUY")
        self.assertEqual(normalized["notional"], 25.0)
        self.assertEqual(normalized["collected_ts"], 1710000001)

    def test_trade_row_normalization_rejects_unknown_asset(self) -> None:
        normalized = normalize_trade_row(
            {
                "asset": "other-token",
                "side": "buy",
                "transactionHash": "0xabc",
                "timestamp": "1710000000",
                "price": "0.25",
                "size": "100",
            },
            market_id="market-1",
            market_slug="btc-up-down",
            yes_token_id="yes-token",
            no_token_id="no-token",
        )

        self.assertIsNone(normalized)


if __name__ == "__main__":
    unittest.main()
