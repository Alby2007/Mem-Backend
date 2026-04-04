# Trading Galaxy — Codemap
> Auto-generated. Regenerate: MCP `run_python codemap_generator.py`

## Directory Index

- **analytics/** — 33 modules: adversarial_stress, adversarial_tester, alerts, anomaly_detector, backtest, calibration_correction, causal_shock_engine, correlation_discovery...
- **ingest/** — 44 modules: acled_adapter, adaptive_scheduler, alpha_vantage_adapter, anomaly_detector_adapter, base, boe_adapter, correlation_discovery_adapter, discovery_pipeline...
- **knowledge/** — 20 modules: authority, causal_graph, confidence_intervals, contradiction, conversation_store, decay, epistemic_adaptation, epistemic_stress...
- **services/** — 6 modules: bot_runner, chat_pipeline, discovery_fleet, paper_trading, scenario_engine, session
- **routes_v2/** — 18 modules: analytics_, auth, billing, chat, discovery, health, ingest_routes, kb...
- **users/** — 2 modules: personal_kb, user_store
- **notifications/** — 8 modules: delivery_scheduler, notify_gate, operator_bot, premarket_briefing, snapshot_formatter, telegram_notifier, tip_formatter, tip_scheduler
- **middleware/** — 8 modules: audit, auth, encryption, fastapi_auth, fastapi_rate_limiter, rate_limiter, stripe_billing, validators
- **llm/** — 4 modules: groq_client, ollama_client, overlay_builder, prompt_builder
- **core/** — 2 modules: levels, tiers
- **root/** — 2 modules: api_v2, extensions

---

## analytics/

### `adversarial_stress`
> analytics/adversarial_stress.py — Adversarial Signal Stress Tester
**Functions:** `_scenario_bear_analyst`, `_scenario_risk_off_regime`, `_scenario_earnings_miss`, `_scenario_macro_flip`, `_scenario_guidance_lowered`, `_scenario_credit_downgrade`, `_compute_conviction_tier`, `_read_baseline`, `_run_scenario`, `run_stress_test`

### `adversarial_tester`
> analytics/adversarial_tester.py — Signal-Level Adversarial Stress Testing
**Classes:** ScenarioOutcome() | AdversarialResult() | AdversarialTester(__init__, stress_test_signal, _robustness_label, _check_earnings_proximity)

### `alerts`
> analytics/alerts.py — Alert Monitor
**Classes:** AlertMonitor(__init__, check)
**Functions:** `_ensure_alerts_table`, `_load_current_kb`, `_load_latest_snapshots`, `_load_recent_edgar_tickers`, `_already_alerted`, `_insert_alert`, `get_alerts`, `mark_alerts_seen`

### `anomaly_detector`
> analytics/anomaly_detector.py — Market State Anomaly Detector
**Classes:** AnomalyDetector(__init__, _conn, _write_atom, _clear_anomaly, _get_snapshots, _build_baseline, _compute_flip_rate, _get_all_ticker_subjects, run)
**Functions:** `_modal`, `_count_deviations`, `_severity`

### `backtest`
> analytics/backtest.py — KB-Native Cross-Sectional Backtest Engine
**Functions:** `_ensure_snapshot_table`, `_read_current_market_regime`, `take_snapshot`, `list_snapshots`, `_load_snapshot`, `_forward_return`, `_safe_float`, `_cohort_stats`, `_weighted_portfolio_return`, `_vs_spy`, `_build_cohorts_and_detail`, `_alpha_result`

### `calibration_correction`
> analytics/calibration_correction.py — Correction Factor for Bot-Only Calibration
**Classes:** CorrectionFactor()
**Functions:** `compute_correction_factors`, `get_global_correction`

### `causal_shock_engine`
> analytics/causal_shock_engine.py — Reactive Causal Shock Propagation
**Classes:** Shock() | ShockPropagationEvent() | CausalShockEngine(__init__, on_atom_written, _detect_shock, _propagate, _seed_concept, _read_prior, _write_causal_atom, _parse_numeric, get_recent_shocks)

### `correlation_discovery`
> analytics/correlation_discovery.py — Cross-Ticker Lead-Lag Discovery
**Classes:** CorrelationDiscovery(__init__, _conn, _ensure_table, _write_atom, _load_ticker_series, _align_series, _avg_snapshot_hours, run)
**Functions:** `_pearson`

### `counterfactual`
> analytics/counterfactual.py — Counterfactual Reasoning Engine
**Functions:** `_compute_conviction_tier`, `_get_baseline`, `_propagate_causal`, `_apply_scenario`, `run_counterfactual`

### `historical_calibration`
> analytics/historical_calibration.py — Historical Signal Calibration Back-population
**Classes:** PatternOutcome() | _AggKey() | _AggBucket() | HistoricalCalibrator(__init__, calibrate_ticker, calibrate_watchlist, _df_to_ohlcv, _atr_from_candles)
**Functions:** `_classify_regime`, `_check_outcome`, `_upsert_calibration`, `main`

### `network_effect_engine`
> analytics/network_effect_engine.py — Network Effect Engine
**Classes:** CoverageTier() | CohortSignal() | TrendingMarket() | NetworkHealthReport() | ConvergenceSignal() | NetworkEffectEngine(__init__, detect_convergence, _kb_signal_direction, _write_convergence_atom)
**Functions:** `_tier_for`, `compute_coverage_tier`, `update_refresh_schedule`, `promote_to_shared_kb`, `detect_cohort_consensus`, `_emit_cohort_atoms`, `compute_trending_markets`, `compute_network_health`

### `observatory_budget`
> analytics/observatory_budget.py — Daily token budget for observatory API calls.
**Functions:** `get_today_str`, `get_budget_row`, `check_and_spend`, `record_spend`

### `observatory_engine`
> analytics/observatory_engine.py
**Classes:** Finding() | ReasonedAction() | _FleetGapSensor(scan) | _BotPerformanceSensor(scan) | _CalibrationSensor(scan) | _DeliverySensor(scan)
**Functions:** `_auto_approve`, `_ensure_observatory_table`

### `opportunity_engine`
> analytics/opportunity_engine.py — On-Demand Investment Opportunity Generation
**Classes:** OpportunityResult() | OpportunityScan()
**Functions:** `classify_intent`, `_load_all_atoms`, `_load_pattern_signals`, `_base_score`, `_scan_broad`, `_scan_momentum`, `_scan_intraday`, `_scan_squeeze`, `_scan_gap_fill`, `_scan_mean_reversion`, `_scan_sector_rotation`, `_scan_macro_gap`

### `pattern_detector`
> analytics/pattern_detector.py — Smart Money Concept Pattern Detector
**Classes:** OHLCV(body_size, total_range, is_bullish, is_bearish, body_ratio) | PatternSignal()
**Functions:** `_avg_body`, `_atr`, `_zone_size_pct`, `_gap_score`, `_recency_score`, `_kb_scores`, `_quality`, `_detect_fvg`, `_update_fvg_status`, `_detect_ifvg`, `_detect_bpr`, `_detect_order_blocks`

### `portfolio`
> analytics/portfolio.py — Portfolio-Level Aggregation
**Functions:** `_safe_float`, `build_portfolio_summary`

### `portfolio_stress_simulator`
> analytics/portfolio_stress_simulator.py — Probability-Weighted Portfolio Stress Simulation
**Classes:** PortfolioStressSimulator(__init__, _conn, _get_open_positions, _get_account_value, _get_ticker_sector, _get_regime_return, _expected_return_for_position, _get_current_state_and_transitions, _map_transition_to_regime, run)
**Functions:** `_normalise_regime`

### `position_calculator`
> analytics/position_calculator.py — Account-Aware Position Sizing
**Classes:** PositionRecommendation()
**Functions:** `calculate_position`
**Imports:** analytics.pattern_detector

### `position_monitor`
> analytics/position_monitor.py — Background Position Monitor
**Classes:** PositionMonitor(__init__, start, stop, _loop)
**Functions:** `_ensure_alerts_table`, `_get_latest_price`, `_get_kb_atom`, `_cooldown_ok`, `_pnl_pct`, `_compute_confidence`, `_is_market_hours`, `_is_actionable_hours`, `_check_triggers`, `_check_profit_triggers`, `_write_alert`, `_send_telegram_alert`

### `prediction_ledger`
> analytics/prediction_ledger.py — Prediction Ledger with Brier Scoring
**Classes:** PredictionLedger(__init__, _ensure_table, _connect, record_prediction, on_price_written, _classify_outcome, _resolve, expire_stale_predictions, get_performance_report, _regime_breakdown, _calibration_curve, get_open_predictions)
**Functions:** `_add_trading_days`

### `regime_history`
> analytics/regime_history.py — Historical Market Regime Classification
**Classes:** RegimeHistoryClassifier(__init__, run)
**Functions:** `_monthly_returns`, `_classify_month`, `main`

### `signal_calibration`
> analytics/signal_calibration.py — Collective Signal Calibration
**Classes:** CalibrationResult()
**Functions:** `_confidence_label`, `_confidence_score`, `_ensure_table`, `update_calibration`, `get_global_baseline`, `_get_user_obs_count`, `get_calibration`

### `signal_decay_predictor`
> analytics/signal_decay_predictor.py — Signal Lifecycle / Decay Predictor
**Classes:** SignalDecayPredictor(__init__, _conn, _write_atom, _get_volatility_regime, _get_calibration_hours, _expected_hours, _get_active_patterns, run)

### `signal_forecaster`
> analytics/signal_forecaster.py — Probabilistic Signal Forecasting
**Classes:** ForecastResult() | _MCResult() | SignalForecaster(__init__, forecast)
**Functions:** `_read_kb_atom`, `_get_iv_rank`, `_get_macro_confirmed`, `_get_short_pct`, `_get_current_regime`, `_run_monte_carlo`
**Imports:** analytics.signal_calibration

### `snapshot_curator`
> analytics/snapshot_curator.py — Personalised Snapshot Assembly
**Classes:** OpportunityCard() | CuratedSnapshot()
**Functions:** `_load_all_signal_atoms`, `_load_macro_atoms`, `_score_opportunity`, `_build_opportunity_card`, `_regime_implication`, `_macro_summary`, `_portfolio_health_section`, `curate_snapshot`

### `state_discretizer`
> analytics/state_discretizer.py — Market State Discretization
**Classes:** CanonicalState(label)
**Functions:** `_map_regime`, `_map_volatility`, `_map_fed`, `_map_sector`, `_map_tension`, `_signal_bias_from_direction`, `_signal_bias_from_sectors`, `_build_state_id`, `discretize`, `discretize_global`, `decode_state_id`

### `state_matcher`
> analytics/state_matcher.py — Historical State Matching
**Classes:** HistoricalPrecedent()
**Functions:** `_is_adjacent`, `state_similarity`, `temporal_weight`, `_bucket_gdelt`, `_extract_current_state`, `_reconstruct_historical_state`, `_aggregate_weighted_outcomes`, `match_historical_state`

### `state_transitions`
> analytics/state_transitions.py — Market State Transition Engine
**Classes:** TransitionProbability() | TransitionForecast() | TransitionEngine(__init__, _conn, build_transitions, get_transition_probabilities, get_current_state_forecast, get_state_statistics)
**Functions:** `_ensure_tables`, `_forward_returns`, `_conf_label`
**Imports:** analytics.state_discretizer

### `strategy_evolution`
> analytics/strategy_evolution.py — Evolutionary selection pressure engine
**Classes:** StrategyEvolution(__init__, evaluate, mutate, crossover, _spawn_replacement, _insert_bot, _update_bot, _log_event, _count_active, _avg_initial_balance, get_discoveries, get_evolution_history, get_fleet_performance)

### `temporal_search`
> analytics/temporal_search.py — Temporal Market State Search Engine
**Classes:** TemporalSearchResult() | TemporalSearchSummary() | TemporalStateSearch(__init__, search_similar_states, search_for_ticker, search_by_natural_language, _current_ticker_state)
**Functions:** `parse_nl_query`, `_snapshot_similarity`, `_temporal_weight_for`, `compute_forward_outcomes`, `_format_period`, `_aggregate_results`

### `thesis_generator`
> analytics/thesis_generator.py — Automatic Thesis Generation from KB Convergence
**Classes:** ThesisGenerator(__init__, _conn, _write_atom, _get_all_tickers, _get_ticker_signals, _score_convergence, _already_has_fresh_thesis, _get_macro_overlay, run)
**Functions:** `_derive_invalidation`

### `universe_expander`
> analytics/universe_expander.py — Universe Expander
**Classes:** UniverseExpansion() | ValidationResult()
**Functions:** `resolve_interest`, `validate_tickers`, `seed_causal_edges`, `bootstrap_ticker`, `bootstrap_ticker_async`, `get_extraction_queue_depth`, `estimate_bootstrap_seconds`

### `user_modeller`
> analytics/user_modeller.py — Portfolio Analysis → User Model
**Functions:** `_read_kb_atoms_for_tickers`, `infer_risk_tolerance`, `infer_sector_affinity`, `infer_holding_style`, `infer_concentration_risk`, `score_portfolio_health`, `build_user_model`
**Imports:** users.user_store

---

## ingest/

### `acled_adapter`
> ingest/acled_adapter.py — ACLED Conflict & Protest Events Ingest Adapter
**Classes:** ACLEDAdapter(__init__, fetch)
**Functions:** `_intensity_label`
**Imports:** ingest.base

### `adaptive_scheduler`
> ingest/adaptive_scheduler.py — Market-Aware Adaptive Scan Frequency
**Classes:** AdaptiveScheduler(__init__, start, stop, _schedule_check, _conn, _get_vol_regime, _get_anomalous_tickers, _get_transition_confidence, _adjust_intervals, _run_check)

### `alpha_vantage_adapter`
> ingest/alpha_vantage_adapter.py — Alpha Vantage News Sentiment Adapter
**Classes:** AlphaVantageAdapter(__init__, fetch)
**Functions:** `_score_to_label`, `_fetch_sentiment`, `_get_watchlist_tickers`
**Imports:** ingest.base

### `anomaly_detector_adapter`
> ingest/anomaly_detector_adapter.py — Scheduler wrapper for AnomalyDetector.
**Classes:** AnomalyDetectorAdapter(__init__, fetch, transform)
**Imports:** ingest.base

### `base`
> ingest/base.py — Ingest Interface Contract (Trading KB)
**Classes:** RawAtom(validate) | BaseIngestAdapter(__init__, fetch, transform, run, push, run_and_push) | ExampleSignalAdapter(__init__, fetch) | ExampleMacroAdapter(__init__, fetch)
**Functions:** `db_connect`, `with_retry`

### `boe_adapter`
> ingest/boe_adapter.py — Bank of England Macro Data Adapter (Trading KB)
**Classes:** BoEAdapter(__init__, fetch)
**Functions:** `_classify_boe_stance`, `_classify_inflation`, `_classify_growth`, `_classify_yield_env`, `_derive_regime`, `_fetch_series`
**Imports:** ingest.base

### `correlation_discovery_adapter`
> ingest/correlation_discovery_adapter.py — Scheduler wrapper for CorrelationDiscovery.
**Classes:** CorrelationDiscoveryAdapter(__init__, fetch, transform)
**Imports:** ingest.base

### `discovery_pipeline`
> ingest/discovery_pipeline.py — Universal Discovery Pipeline
**Classes:** DiscoveryResult() | DiscoveryPipeline(__init__, assess_staleness, discover, _run_stage, _run_price, _run_historical, _run_options, _run_enrichment, _run_patterns, _run_short_interest, _run_flow, _fact_count, _update_coverage, _log_discovery)
**Functions:** `_ensure_discovery_log`, `_assess_staleness`, `_stages_needed`, `_is_lse_ticker`

### `dynamic_watchlist`
> ingest/dynamic_watchlist.py — Dynamic Watchlist Manager
**Classes:** DynamicWatchlistManager(get_active_tickers, get_pattern_tickers, get_priority_tickers, get_user_tickers, add_tickers, remove_ticker, get_bootstrap_status)
**Functions:** `coverage_tier_for`, `_meets_promotion_criteria`, `_ensure_hybrid_tables`

### `earnings_calendar_adapter`
> ingest/earnings_calendar_adapter.py — Earnings Calendar Enrichment Adapter
**Classes:** EarningsCalendarAdapter(__init__, fetch, _load_upcoming_earnings)
**Functions:** `_days_until`, `_implied_move_from_options`, `_implied_move_from_vol`
**Imports:** ingest.base

### `economic_calendar_adapter`
> ingest/economic_calendar_adapter.py — Economic Calendar Event Risk Adapter
**Classes:** EconomicCalendarAdapter(__init__, fetch)
**Functions:** `_days_until`, `_next_fomc`, `_fetch_fred_release_dates`, `_fallback_event_dates`
**Imports:** ingest.base

### `edgar_adapter`
> ingest/edgar_adapter.py — SEC EDGAR Ingest Adapter (Trading KB)
**Classes:** EDGARAdapter(__init__, fetch, _fetch_symbol, _filing_to_atoms)
**Functions:** `_get_headers`, `_ticker_to_cik`, `_fetch_recent_filings`
**Imports:** ingest.base

### `edgar_realtime_adapter`
> ingest/edgar_realtime_adapter.py — Real-Time SEC EDGAR 8-K Poller
**Classes:** EDGARRealtimeAdapter(__init__, fetch)
**Functions:** `_ensure_seen_table`, `_load_seen_ids`, `_mark_seen`, `_match_ticker`, `_parse_feed`
**Imports:** ingest.base, ingest.rss_adapter

### `eia_adapter`
> ingest/eia_adapter.py — U.S. Energy Information Administration (EIA) Ingest Adapter
**Classes:** EIAAdapter(__init__, fetch, _derive_energy_regime)
**Functions:** `_eia_fetch`, `_trend`
**Imports:** ingest.base

### `fca_short_interest_adapter`
> ingest/fca_short_interest_adapter.py — FCA Short Selling Register Adapter
**Classes:** FCAShortInterestAdapter(__init__, fetch)
**Functions:** `_resolve_ticker`, `_classify_squeeze`, `_cross_ref_signal`, `_cross_ref_tension`
**Imports:** ingest.base

### `finra_short_interest_adapter`
> ingest/finra_short_interest_adapter.py — FINRA Short Interest Adapter (US)
**Classes:** FINRAShortInterestAdapter(__init__, fetch)
**Functions:** `_fetch_finra_api`, `_squeeze_risk`, `_vs_signal`
**Imports:** base

### `fred_adapter`
> ingest/fred_adapter.py — Federal Reserve (FRED) Ingest Adapter (Trading KB)
**Classes:** _FredAuthError() | FREDAdapter(__init__, fetch, _fetch_atoms)
**Functions:** `_classify_stance`, `_classify_inflation`, `_classify_growth`, `_derive_regime`, `_get_latest_value`
**Imports:** ingest.base

### `gdelt_adapter`
> ingest/gdelt_adapter.py — GDELT GKG Bilateral Tension Ingest Adapter
**Classes:** GDELTAdapter(__init__, fetch)
**Functions:** `_apply_geo_lexicon`, `_gdelt_tone_query`, `_trend_label`, `_risk_tier`
**Imports:** ingest.base

### `geo_exposure`
> ingest/geo_exposure.py — Ticker-to-Geopolitical-Region Exposure Config

### `gpr_adapter`
> ingest/gpr_adapter.py — Caldara-Iacoviello Geopolitical Risk (GPR) Index Adapter
**Classes:** GPRAdapter(__init__, fetch)
**Functions:** `_level_label`, `_trend_label`, `_fetch_xls_bytes`, `_excel_serial_to_period`, `_parse_xls`, `_db_last_gpr_period`, `_db_last_gpr_value`
**Imports:** ingest.base

### `historical_adapter`
> ingest/historical_adapter.py — Historical Summary Backfill Adapter
**Classes:** HistoricalBackfillAdapter(__init__, fetch, _extract_close, _extract_volume, _compute_atoms)
**Functions:** `_pct_return`, `_realised_vol`, `_drawdown_from_high`, `_max_drawdown`
**Imports:** ingest.base

### `historical_calibration_adapter`
> ingest/historical_calibration_adapter.py — Scheduled Historical Calibration
**Classes:** HistoricalCalibrationAdapter(__init__, fetch, transform)
**Imports:** ingest.base

### `insider_adapter`
> ingest/insider_adapter.py — Form 4 Insider Transaction Adapter
**Classes:** InsiderAdapter(__init__, fetch)
**Functions:** `_load_cik_map`, `_get_cik`, `_fetch_form4_filings`, `_parse_form4_document`, `_classify_conviction`, `_classify_role_priority`
**Imports:** ingest.base

### `llm_extraction_adapter`
> ingest/llm_extraction_adapter.py — LLM-Based Atom Extraction Adapter
**Classes:** LLMExtractionAdapter(__init__, _llm_call, _any_llm_available, fetch)
**Functions:** `_build_prompt`, `_confidence_from_language`, `_parse_llm_atoms`
**Imports:** ingest.base, ingest.rss_adapter

### `lse_flow_adapter`
> ingest/lse_flow_adapter.py — LSE Institutional Order Flow Adapter
**Classes:** LSEFlowAdapter(__init__, fetch, _get_tickers)
**Functions:** `_compute_flow_signals`, `_fetch_candles`
**Imports:** ingest.base

### `options_adapter`
> ingest/options_adapter.py — Options Market Data Adapter
**Classes:** OptionsAdapter(__init__, fetch, _fetch_one, _fetch_spy_skew)
**Functions:** `_iv_rank_from_chain`, `_classify_options_regime`, `_put_call_ratio`, `_detect_sweep`, `_compute_skew`, `_classify_skew_regime`, `_classify_tail_risk`, `_fetch_chain_data`
**Imports:** ingest.base

### `pattern_adapter`
> ingest/pattern_adapter.py — OHLCV Pattern Detection Adapter
**Classes:** PatternAdapter(__init__, _get_tickers, run, _process_ticker)
**Functions:** `_read_kb_context`, `_read_ohlcv_cache`, `_fetch_ohlcv`, `_resample_4h`, `_pattern_exists`, `_dedup_existing_patterns`, `_kb_last_price`, `_update_existing_patterns`
**Imports:** analytics.pattern_detector, ingest.base, users.user_store

### `polygon_options_adapter`
> ingest/polygon_options_adapter.py — Polygon/Massive Options Greeks Adapter
**Classes:** PolygonOptionsAdapter(__init__, fetch, _fetch_one)
**Functions:** `_api_key`, `_nearest_expiry_atm_contracts`, `_extract_greeks`, `_put_call_oi`, `_gamma_exposure`
**Imports:** ingest.base

### `polygon_price_adapter`
> ingest/polygon_price_adapter.py — Polygon Price & Fundamentals Adapter
**Classes:** PolygonPriceAdapter(__init__, _should_run, _mark_run, fetch, _grouped_daily, _ticker_details, _financials, _news, _dividends_splits)
**Functions:** `_load_us_tickers`, `get_us_polygon_tickers`, `_api_key`, `_get`, `_market_cap_tier`
**Imports:** ingest.base

### `polymarket_adapter`
> ingest/polymarket_adapter.py — Polymarket Prediction Market Adapter
**Classes:** PolymarketAdapter(__init__, fetch)
**Functions:** `_prob_label`, `_fetch_markets`, `_best_market`, `_extract_yes_prob`
**Imports:** ingest.base

### `rss_adapter`
> ingest/rss_adapter.py — RSS News Ingest Adapter (Trading KB)
**Classes:** RSSAdapter(__init__, fetch, _fetch_feed)
**Functions:** `_extract_tickers`, `_is_negative`, `_ensure_extraction_queue`
**Imports:** ingest.base

### `scheduler`
> ingest/scheduler.py — Automated Ingest Scheduler (Trading KB)
**Classes:** AdapterStatus(to_dict) | IngestScheduler(__init__, register, start, stop, get_status, update_interval, run_now, _schedule, _run_adapter)
**Imports:** ingest.base

### `sector_rotation_adapter`
> ingest/sector_rotation_adapter.py — Sector Rotation Signal Adapter
**Classes:** SectorRotationAdapter(__init__, fetch)
**Functions:** `_read_return_1m`, `_read_ticker_sector`, `_read_portfolio_tickers`
**Imports:** ingest.base

### `seed_sync`
> ingest/seed_sync.py — Centralised KB seed sync client.
**Classes:** SeedSyncClient(__init__, start, stop, _loop, _safe_check, _check_and_apply, _apply_seed, _purge_fake_atoms, _read_tag, _write_tag)

### `signal_decay_adapter`
> ingest/signal_decay_adapter.py — Scheduler wrapper for SignalDecayPredictor.
**Classes:** SignalDecayAdapter(__init__, fetch, transform)
**Imports:** ingest.base

### `signal_enrichment_adapter`
> ingest/signal_enrichment_adapter.py — Second-Order Signal Enrichment
**Classes:** SignalEnrichmentAdapter(__init__, fetch)
**Functions:** `_read_kb_atoms`, `_compute_skew_filter_atoms`, `_compute_position_sizing_atoms`, `_compute_invalidation_atoms`, `_classify_price_regime`, `_classify_signal_quality`, `_classify_macro_confirmation`, `_classify_market_regime`, `_classify_earnings_proximity`, `_compute_news_sentiment`, `_read_geo_atoms`, `_compute_geo_risk_atoms`
**Imports:** ingest.base

### `state_snapshot_adapter`
> ingest/state_snapshot_adapter.py — Full Market State Snapshot Adapter
**Classes:** StateSnapshotAdapter(__init__, _get_watchlist, fetch, transform)
**Functions:** `_bucket_gdelt`, `_bucket_gpr`, `ensure_snapshot_table`, `_read_ticker_state`, `_read_global_state`, `_write_snapshots`
**Imports:** ingest.base

### `strategy_evolution_adapter`
> ingest/strategy_evolution_adapter.py — 6-hour scheduler wrapper for StrategyEvolution.
**Classes:** StrategyEvolutionAdapter(__init__, fetch, transform)
**Imports:** ingest.base

### `thesis_generator_adapter`
> ingest/thesis_generator_adapter.py — Scheduler wrapper for ThesisGenerator.
**Classes:** ThesisGeneratorAdapter(__init__, fetch, transform)
**Imports:** ingest.base

### `transition_builder_adapter`
> ingest/transition_builder_adapter.py — Daily State Transition Builder
**Classes:** TransitionBuilderAdapter(__init__, _get_watchlist, fetch, transform)
**Imports:** ingest.base

### `ucdp_adapter`
> ingest/ucdp_adapter.py — Country Conflict Signal Adapter (GDELT-derived)
**Classes:** UCDPAdapter(__init__, fetch)
**Functions:** `_gdelt_article_count`
**Imports:** ingest.base

### `usgs_adapter`
> ingest/usgs_adapter.py — USGS Earthquake Feed Ingest Adapter
**Classes:** _Region() | USGSAdapter(__init__, fetch)
**Functions:** `_in_region`, `_haversine_km`
**Imports:** ingest.base

### `yfinance_adapter`
> ingest/yfinance_adapter.py — Yahoo Finance Ingest Adapter (Trading KB)
**Classes:** YFinanceAdapter(__init__, _load_delisted_from_db, _mark_delisted, _clear_yf_session, fetch, _fetch_chart_candles, _cache_ohlcv_candles, _bulk_download_prices, _parallel_info_fetch, _fetch_info_atoms, _info_to_atoms, _etf_atoms)
**Functions:** `_get_us_polygon_tickers`, `_market_cap_tier`, `_volatility_regime`, `_direction_from_target`
**Imports:** ingest.base

### `yield_curve_adapter`
> ingest/yield_curve_adapter.py — Treasury Yield Curve Adapter (Polygon)
**Classes:** YieldCurveAdapter(__init__, fetch)
**Functions:** `_api_key`, `_fetch_last_two_closes`, `_pct_change`, `_classify_regime`, `_classify_slope`
**Imports:** base

---

## knowledge/

### `authority`
> knowledge/authority.py — Source Authority Weights (Trading KB)
**Functions:** `get_authority`, `effective_score`, `conflict_winner`

### `causal_graph`
> knowledge/causal_graph.py — Causal Graph Layer
**Functions:** `ensure_causal_edges_table`, `_load_adjacency`, `traverse_causal`, `_compute_chain_confidence`, `_find_live_kb_tickers`, `add_causal_edge`, `list_causal_edges`

### `confidence_intervals`
> knowledge/confidence_intervals.py — Bayesian Confidence Intervals
**Functions:** `ensure_confidence_columns`, `welford_update`, `widen_for_conflict`, `get_confidence_interval`, `get_all_confidence_intervals`, `update_atom_confidence`, `widen_atom_confidence`

### `contradiction`
> knowledge/contradiction.py — Contradiction Detection
**Classes:** ConflictResult() | ContradictionDetector(check, _row_to_dict, _mark_superseded, _log_conflict)
**Functions:** `ensure_conflicts_table`, `get_detector`
**Imports:** knowledge.authority

### `conversation_store`
> knowledge/conversation_store.py — Persistent conversation history for Trading Galaxy.
**Classes:** ConversationStore(__init__, _conn, _ensure_tables, get_or_create_session, get_session, delete_session_messages, add_message, get_recent_messages_for_context, get_message_pair, add_turn_atoms, get_salient_atoms, mark_atom_graduated, get_atoms_with_status, get_timeline, get_total_turn_count, get_cognitive_metrics)
**Functions:** `session_id_for_user`

### `decay`
> knowledge/decay.py — Confidence Decay by Age (Trading KB)
**Classes:** DecayWorker(__init__, start, stop, run_once, _loop, _update_all)
**Functions:** `_get_half_life`, `decay_confidence`, `get_effective_confidence`, `ensure_decay_column`, `get_decay_worker`
**Imports:** knowledge.authority

### `epistemic_adaptation`
> knowledge/epistemic_adaptation.py — Epistemic Adaptation Engine (Phase 3)
**Classes:** AdaptationNudges(is_active, debug_str) | EpistemicAdaptationEngine(__init__, compute, _queue_refresh, _queue_synthesis, _log_consolidation, _consolidation_count, _classify_and_log)
**Functions:** `ensure_adaptation_tables`, `get_adaptation_engine`

### `epistemic_stress`
> knowledge/epistemic_stress.py — Epistemic Stress Signaling
**Classes:** EpistemicStressReport(debug_str)
**Functions:** `_compute_supersession_density`, `_compute_decay_pressure`, `_compute_authority_conflict`, `_compute_conflict_cluster`, `_compute_domain_entropy`, `compute_structural_stress`, `compute_stress`

### `graph`
> Trading Knowledge Graph — WAL mode, Trading Taxonomy, and Comprehensive Search
**Classes:** TradingKnowledgeGraph(__init__, set_shock_engine, set_ledger, set_thesis_monitor, thread_local_conn, _initialize_db, _init_taxonomy, add_fact, search, _sanitize_query, _fallback_search, query, get_context, get_stats)
**Functions:** `_ensure_hit_count_column`

### `graph_enhanced`
> Enhanced Knowledge Graph with WAL mode, Taxonomy, and Comprehensive Search
**Classes:** EnhancedKnowledgeGraph(__init__, _initialize_db, _init_taxonomy, add_fact, search, _sanitize_query, _fallback_search, query, get_context, get_stats)

### `graph_retrieval`
> Trading KB Graph Retrieval — atom-to-graph pipeline
**Functions:** `_build_node_scores`, `atoms_to_graph`, `compute_degree_centrality`, `compute_pagerank`, `bfs_path`, `find_concept_clusters`, `extract_query_concepts`, `build_graph_context`, `what_do_i_know_about`

### `graph_v2`
> Enhanced Knowledge Graph with Versioning and Conflict Resolution
**Classes:** KnowledgeGraphV2(__init__)

### `kb_domain_schemas`
> knowledge/kb_domain_schemas.py — Domain predicate schemas (ontology layer) — Trading KB
**Functions:** `detect_topic_domain`, `missing_schema_predicates`, `schema_completeness`

### `kb_insufficiency_classifier`
> knowledge/kb_insufficiency_classifier.py — KB Insufficiency Classifier (Phase 4)
**Classes:** InsufficiencyType() | InsufficiencyDiagnosis(primary_type, debug_str, to_json)
**Functions:** `_extract_topic_signals`, `_jaccard_similarity_sample`, `classify_insufficiency`

### `kb_repair_executor`
> knowledge/kb_repair_executor.py — KB Repair Executor (Phase 6)
**Classes:** SignalSnapshot(to_dict) | DivergenceReport(to_dict) | ExecutionResult(debug_str) | RollbackResult() | ImpactScore()
**Functions:** `ensure_executor_tables`, `_snapshot_signals`, `_compute_divergence`, `_store_rollback_snapshot`, `_apply_zero_ids`, `_apply_reweight_sources`, `_topic_domain_entropy`, `_apply_merge_atoms`, `_apply_introduce_predicates`, `_apply_restore_atoms`, `_write_execution_log`, `execute_repair`

### `kb_repair_proposals`
> knowledge/kb_repair_proposals.py — KB Repair Proposals (Phase 5)
**Classes:** RepairStrategy() | RepairPreview(to_dict) | RepairSimulation(to_dict) | ValidationMetric(to_dict) | RepairProposal(debug_str, to_db_row)
**Functions:** `_fetch_topic_atoms`, `_propose_ingest_missing`, `_propose_resolve_conflicts`, `_propose_merge_atoms`, `_is_code_token`, `_propose_introduce_predicates`, `_propose_reweight_sources`, `_propose_deduplicate`, `_propose_split_domain`, `_propose_restore_atoms`, `_propose_manual_review`, `_ensure_type_map`

### `kb_validation`
> knowledge/kb_validation.py — KB Epistemic Validation & Governance (Phase 7)
**Classes:** ValidationIssue() | ValidationReport(_compute_severity) | GovernanceVerdict(to_dict)
**Functions:** `_fetch_atoms`, `validate_schema`, `validate_semantics`, `validate_cross_topic`, `validate_all`, `_ensure_governance_metrics_table`, `_record_governance_metrics`, `_compute_adaptive_threshold`, `governance_verdict`

### `thesis_builder`
> knowledge/thesis_builder.py — Natural Language Thesis Builder + Monitor
**Classes:** ThesisResult() | ThesisEvaluation() | ThesisBuilder(__init__, detect_thesis_intent, build, evaluate, list_user_theses, _evaluate_evidence, _is_supporting, _is_contradicting, _derive_invalidation, _write_thesis_atoms, _upsert_thesis_index, _get_thesis_index_row, _ensure_tables, _connect) | ThesisMonitor(__init__, on_atom_written, _maybe_refresh_index, _refresh_index, _check_thesis, _fire_alert, _recently_alerted, _mark_alert_sent, _get_user_chat_id)

### `working_memory`
> knowledge/working_memory.py — On-Demand Fetch → Working Memory → KB Commit Loop
**Classes:** CommitResult() | _Session() | WorkingMemory(__init__, open_session, close_without_commit, fetch_on_demand, web_search_on_demand, _ddg_search, _google_news_fallback, get_session_snippet, get_fetched_tickers, commit_session)
**Functions:** `_is_known_ticker`, `parse_llm_response`, `_extract_ticker_hint`, `kb_has_atoms`, `_price_regime_from_ratio`, `_direction_from_target`, `_should_commit`

### `working_state`
> knowledge/working_state.py — Cross-Session Working State Persistence
**Classes:** WorkingStateSnapshot(format_for_context) | WorkingStateStore(__init__, _get_conn, maybe_persist, _write, _prune, get_recent, format_prior_context)
**Functions:** `ensure_working_state_table`, `get_working_state_store`

---

## services/

### `bot_runner`
> services/bot_runner.py — Evolutionary Strategy Bot Runner
**Classes:** BotRunner(__init__, _ensure_tables, count_bots, seed_fleet, start_bot, stop_bot, kill_bot, start_bot_position_monitor, stop_bot_position_monitor, _bot_position_monitor_loop, _run_bot_position_monitor_cycle, restore_bots, stop_all, _build_filtered_query, _bot_scan_loop, _bot_scan_once, _monitor_bot_positions, _write_bot_equity, _get_ticker_atom, get_bot_performance, list_bots, create_manual_bot)
**Functions:** `_hash_genome`, `_name_genome`, `generate_random_genome`, `_make_seed_templates`

### `chat_pipeline`
> services/chat_pipeline.py — KB-grounded chat pipeline.
**Functions:** `compute_market_stress`, `_query_wants_live`, `_wants_portfolio`, `_is_tip_request`, `_detect_plain_english_intent`, `_detect_thesis_validity_intent`, `_resolve_thesis_for_message`, `_build_thesis_context_string`, `sid_for_user`, `_get_trader_level`, `_tier_atom_limit`, `_boost_watchlist_atoms`

### `discovery_fleet`
> services/discovery_fleet.py — Internal discovery fleet management.
**Functions:** `ensure_discovery_user`, `_build_grid`, `seed_discovery_fleet`, `get_discovery_report`, `get_discovery_status`

### `paper_trading`
> services/paper_trading.py — Paper trading business logic.
**Classes:** PaperAgentAdapter(run)
**Functions:** `_is_market_open`, `ensure_paper_tables`, `paper_tier_check`, `fetch_live_prices`, `compute_pnl_r`, `get_account`, `get_equity_log`, `update_account_size`, `list_positions`, `open_position`, `close_position`, `monitor_positions`

### `scenario_engine`
> services/scenario_engine.py — Read-only Scenario Testing Engine
**Classes:** ScenarioResult()
**Functions:** `_resolve_seed`, `_geometric_mean`, `_filter_portfolio_impact`, `run_scenario`
**Imports:** difflib

### `session`
> services/session.py — Thread-safe session state manager.
**Classes:** SessionManager(__init__, _touch, _maybe_cleanup, _cleanup, get_streak, set_streak, reset_streak, all_streaks, active_streak_count, total_streak_count, has_streak, get_tickers, set_tickers, has_tickers, pop_tickers, get_portfolio_tickers, set_portfolio_tickers, pop_portfolio_tickers, clear_session)

---

## routes_v2/

### `analytics_`
> routes_v2/analytics_.py — Phase 6: analytics endpoints.
**Classes:** MarkSeenRequest() | StressTestRequest() | CounterfactualRequest() | SignalStressRequest()
**Imports:** middleware.fastapi_auth

### `auth`
> routes_v2/auth.py — Phase 2: auth endpoints.
**Classes:** RegisterRequest() | LoginRequest() | RefreshRequest() | ChangePasswordRequest() | TelegramVerifyRequest() | SetDevRequest()
**Functions:** `_set_auth_cookies`, `_clear_auth_cookies`
**Imports:** middleware.fastapi_auth, middleware.fastapi_rate_limiter

### `billing`
> routes_v2/billing.py — Phase 4: Stripe billing endpoints.
**Classes:** CheckoutRequest()
**Imports:** middleware.fastapi_auth

### `chat`
> routes_v2/chat.py — Phase 3: chat endpoint.
**Classes:** ChatRequest()
**Functions:** `_check_chat_quota`
**Imports:** middleware.fastapi_auth, middleware.fastapi_rate_limiter

### `discovery`
> routes_v2/discovery.py — Internal discovery fleet API routes.
**Functions:** `_internal_auth`, `_get_runner`, `_dev_gate`
**Imports:** middleware.fastapi_auth, services.discovery_fleet

### `health`
> routes_v2/health.py — Phase 1: health endpoints.

### `ingest_routes`
> routes_v2/ingest_routes.py — Phase 6: ingest/calibration/discovery endpoints.
**Classes:** RunAllRequest() | HistoricalRequest() | CalibrationRequest() | RegimeHistoryRequest() | DiscoverRequest()
**Imports:** middleware.fastapi_auth

### `kb`
> routes_v2/kb.py — Phase 6: knowledge base endpoints.
**Classes:** IngestAtom() | IngestRequest() | RetrieveRequest()
**Imports:** middleware.fastapi_auth

### `markets`
> routes_v2/markets.py — Phase 6: markets endpoints.
**Functions:** `_get_market_regime`, `_vis_norm_sector`
**Imports:** middleware.fastapi_auth

### `network`
> routes_v2/network.py — Phase 6: network effect endpoints.
**Imports:** middleware.fastapi_auth

### `paper`
> routes_v2/paper.py — Phase 5: paper trading endpoints.
**Classes:** UpdateAccountRequest() | OpenPositionRequest() | ClosePositionRequest() | CreateBotRequest() | UpdateBotRequest() | ReseedRequest()
**Functions:** `_tier_gate`, `_get_runner`
**Imports:** middleware.fastapi_auth, services

### `patterns`
> routes_v2/patterns.py — Phase 6: pattern endpoints.
**Classes:** PatternFeedbackRequest() | FeedbackRequest() | TipFeedbackRequest() | PositionUpdateRequest()
**Imports:** middleware.fastapi_auth

### `scenario`
> routes_v2/scenario.py — Scenario testing endpoints.
**Classes:** ScenarioRequest()
**Imports:** middleware.fastapi_auth, middleware.fastapi_rate_limiter, services.scenario_engine

### `status`

### `telegram`
> routes_v2/telegram.py — Phase 8: Telegram bot webhook, callback, and registration.
**Classes:** RegisterRequest()
**Functions:** `_tg_api`, `_handle_tg_message`, `_handle_tg_callback`

### `thesis`
> routes_v2/thesis.py — Phase 6: thesis builder endpoints.
**Classes:** ThesisBuildRequest()
**Imports:** middleware.fastapi_auth

### `users`
> routes_v2/users.py — Phase 7: user management endpoints.
**Classes:** OnboardingRequest() | PortfolioRequest() | TipConfigRequest() | NotificationPrefsRequest() | TradingPrefsRequest() | StylePrefsRequest()
**Functions:** `_build_prefs_confirmation`
**Imports:** middleware.fastapi_auth, middleware.fastapi_rate_limiter

### `waitlist`
> routes_v2/waitlist.py — Phase 6: waitlist endpoints.
**Classes:** WaitlistRequest()
**Functions:** `_ensure_waitlist_table`, `_notify_waitlist_telegram`
**Imports:** middleware.fastapi_rate_limiter

---

## users/

### `personal_kb`
> users/personal_kb.py — Personal Knowledge Base Layer
**Classes:** PersonalContext() | PersonalKB(_ensure_table, write_atom, read_atoms, get_context_document, infer_and_write_from_portfolio, update_from_feedback, update_from_engagement, write_universe_atoms)
**Functions:** `write_atom`, `read_atoms`, `get_context_document`, `infer_and_write_from_portfolio`, `update_from_feedback`, `update_from_engagement`, `write_universe_atoms`

### `user_store`
> users/user_store.py — User Management Store
**Functions:** `ensure_user_tables`, `create_user`, `get_user`, `set_user_dev`, `get_user_by_chat_id`, `update_preferences`, `get_style_prefs`, `update_style_prefs`, `upsert_portfolio`, `upsert_single_holding`, `get_portfolio`, `_ticker_to_name`

---

## notifications/

### `delivery_scheduler`
> notifications/delivery_scheduler.py — Timezone-Aware Delivery Scheduler
**Classes:** DeliveryScheduler(__init__, start, stop, is_running, _run, _check_all_users)
**Functions:** `_get_local_now`, `_should_deliver`, `_deliver_to_user`

### `notify_gate`
> notifications/notify_gate.py — Shared delivery gate for all schedulers
**Functions:** `_get_local_now`, `_normalise_days`, `should_notify`

### `operator_bot`
> notifications/operator_bot.py
**Functions:** `_e`, `_send`, `send_observatory_report`, `handle_callback`, `_execute_write`

### `premarket_briefing`
> notifications/premarket_briefing.py — Pre-Market Narrative Briefing Generator
**Functions:** `_rank_atoms`, `_truncate_at_paragraph_boundary`, `_format_portfolio_context`, `_format_position_list`, `_build_ranked_snippet`, `_esc`, `_build_your_week_section`, `_build_kb_performance_section`, `_build_regime_outlook_section`, `generate_premarket_narrative`, `_build_calibration_update_section`, `_build_fleet_discoveries_section`

### `snapshot_formatter`
> notifications/snapshot_formatter.py — CuratedSnapshot → Telegram Message
**Functions:** `_escape_mdv2`, `_section_portfolio`, `_section_market`, `_section_opportunities`, `_section_avoid`, `format_snapshot`, `snapshot_to_dict`

### `telegram_notifier`
> notifications/telegram_notifier.py — Telegram Bot API Wrapper
**Classes:** TelegramNotifier(__init__, is_configured, send, send_test, send_plain)
**Functions:** `escape_mdv2`

### `tip_formatter`
> notifications/tip_formatter.py — Pattern Tip Telegram Formatter
**Functions:** `_escape_mdv2`, `_fmt_price`, `_fmt_currency`, `_conviction_line`, `_regime_line`, `_signal_dir_line`, `pattern_allowed_for_tier`, `timeframe_allowed_for_tier`, `fetch_greeks`, `_parse_skew_filter`, `_fmt_pct`, `_forecast_block`
**Imports:** analytics.pattern_detector, analytics.position_calculator, core.tiers

### `tip_scheduler`
> notifications/tip_scheduler.py — User Delivery-Time-Aware Tip Scheduler
**Classes:** TipScheduler(__init__, start, stop, _loop)
**Functions:** `_get_local_now`, `_week_monday`, `_get_briefing_mode`, `_should_send_batch`, `_scan_candidates`, `_pick_best_pattern`, `_validate_tip`, `_pick_batch`, `_check_monday_status`, `_get_kb_price`, `_deliver_tip_to_user`, `_migrate_pro_to_premium`

---

## middleware/

### `audit`
> middleware/audit.py — Audit logging
**Functions:** `ensure_audit_table`, `log_audit_event`, `get_audit_log`

### `auth`
> middleware/auth.py — JWT Authentication
**Functions:** `ensure_user_auth_table`, `_make_access_token`, `_make_token`, `_decode_token`, `issue_refresh_token`, `rotate_refresh_token`, `_hash_password`, `_init_dummy_hash`, `_check_password`, `require_auth`, `assert_self`, `register_user`
**Imports:** flask

### `encryption`
> middleware/encryption.py — Field-level encryption helpers
**Functions:** `encrypt_field`, `decrypt_field`, `encryption_enabled`

### `fastapi_auth`
> middleware/fastapi_auth.py — FastAPI auth dependencies.
**Imports:** middleware.auth

### `fastapi_rate_limiter`
> middleware/fastapi_rate_limiter.py — slowapi rate limiter for FastAPI.
**Functions:** `_rate_limit_key`
**Imports:** slowapi, slowapi.util

### `rate_limiter`
> middleware/rate_limiter.py — Flask-Limiter configuration
**Functions:** `_get_user_or_ip`

### `stripe_billing`
> middleware/stripe_billing.py — Stripe Checkout + webhook handling.
**Functions:** `_sk`, `_price_id`, `create_checkout_session`, `create_portal_session`, `_tier_from_event`, `_user_id_from_event`, `handle_webhook`, `_set_user_tier`, `_store_stripe_customer_id`, `_handle_checkout_completed`, `_handle_subscription_active`, `_handle_subscription_cancelled`

### `validators`
> middleware/validators.py — Input validation for API boundary
**Classes:** ValidationResult()
**Functions:** `_is_positive_number`, `_is_non_negative_number`, `validate_portfolio_submission`, `validate_onboarding`, `validate_tip_config`, `validate_ingest_atom`, `validate_feedback`, `validate_register`

---

## llm/

### `groq_client`
> llm/groq_client.py — Thin wrapper around the Groq REST API.
**Functions:** `_api_key`, `chat`, `is_available`

### `ollama_client`
> llm/ollama_client.py — Thin wrapper around the local Ollama REST API.
**Functions:** `chat`, `list_models`, `warmup`, `chat_vision`, `is_available`

### `overlay_builder`
> llm/overlay_builder.py — Overlay Card Assembly for Active Copilot Mode
**Functions:** `_load_kb_subjects`, `extract_tickers`, `_load_atoms_for_tickers`, `_build_signal_summary_card`, `_build_causal_context_card`, `_build_stress_flag_card`, `build_overlay_cards`

### `prompt_builder`
> llm/prompt_builder.py — System prompt and user-turn assembly for the KB-grounded chat layer.
**Functions:** `build`

---

## core/

### `levels`
> core/levels.py — Trader Experience Level Configuration
**Functions:** `get_level`, `tip_format`, `show_greeks`, `max_risk_pct`

### `tiers`
> core/tiers.py — Single source of truth for tier configuration.
**Functions:** `get_tier`, `check_feature`, `_next_tier`

---

## root/

### `api_v2`
> api_v2.py — FastAPI application factory.
**Functions:** `create_fastapi_app`
**Imports:** slowapi, slowapi.errors

### `extensions`
> extensions.py — Single source of truth for feature flags, shared objects, and imports.
**Functions:** `llm_chat`, `get_user_tier_for_request`, `require_feature`
**Imports:** knowledge, knowledge.decay, services.session

---
