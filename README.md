# trading-pipeline
A trading data pipeline and strategy tester written in Python. **Under heavy development.**

1. Data is exported from MetaTrader via a custom indicator.
2. Apache Airflow DAG uploads these to Google Cloud Storage and inserts them into a staging table in BigQuery.
3. Separate Apache Airflow DAG creates tables in BigQuery per symbol and timeframe as specified in config.py.
