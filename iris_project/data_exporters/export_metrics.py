import os
import pandas as pd

if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(func): return func
if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func): return func


@data_exporter
def export_metrics(df, *args, **kwargs):
    """Custom block (C4): exports evaluation metrics table to a CSV file."""
    df.to_csv('iris_metrics.csv', index=False)
    print('Exported evaluation metrics to iris_metrics.csv')


@test
def test_metrics_saved(output, *args):
    """Test for C4: verify exported file exists and has expected rows."""
    assert os.path.exists('iris_metrics.csv'), 'iris_metrics.csv file was not created'
    saved = pd.read_csv('iris_metrics.csv')
    assert len(saved) >= 4, 'Metrics table must have at least 4 metrics'

