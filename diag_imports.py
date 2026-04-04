import sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

tests = [
    ('ingest.thesis_generator_adapter', 'ThesisGeneratorAdapter'),
    ('ingest.anomaly_detector_adapter', 'AnomalyDetectorAdapter'),
    ('ingest.signal_decay_adapter', 'SignalDecayAdapter'),
    ('ingest.correlation_discovery_adapter', 'CorrelationDiscoveryAdapter'),
    ('ingest.adaptive_scheduler', 'AdaptiveScheduler'),
    ('analytics.thesis_generator', 'ThesisGenerator'),
    ('analytics.anomaly_detector', 'AnomalyDetector'),
    ('analytics.correlation_discovery', 'CorrelationDiscovery'),
    ('analytics.signal_decay_predictor', 'SignalDecayPredictor'),
    ('analytics.portfolio_stress_simulator', 'PortfolioStressSimulator'),
]

ok = 0
for mod, cls in tests:
    try:
        m = __import__(mod, fromlist=[cls])
        getattr(m, cls)
        print(f'  OK  {mod}.{cls}')
        ok += 1
    except Exception as e:
        print(f'  FAIL {mod}.{cls}: {e}')

print(f'\n{ok}/{len(tests)} modules imported successfully')
