# Plan: Binance DataTool Consumer SDK

## 1. Overview
The `binance-datatool-sdk` is a lightweight, read-only Python library designed for quant researchers to consume production-ready datasets published to Hugging Face. It leverages DuckDB and the `ducklake` extension to provide a zero-copy, high-performance experience.

## 2. Core Features
- **Seamless HF Integration**: One-line download/attachment of versioned datasets from Hugging Face Hub.
- **DuckLake Native**: Automatically handles `ducklake` extension installation and catalog attachment.
- **Medallion Access**: Provides typed access to `registry`, `silver`, and `gold` layers.
- **Survivorship-Bias-Free Discovery**: Built-in methods to resolve point-in-time universes for backtesting.

## 3. Planned API (Mockup)

```python
from binance_datatool_sdk import connect

# Connect to a versioned dataset on Hugging Face
lake = connect("org/top50-backtesting-v1")

# 1. Get the tradable universe for a specific date (Zero Survivorship Bias)
universe = lake.universe.get_top_50(
    trade_type='spot',
    as_of='2024-01-01'
)

# 2. Query Silver data directly via DuckDB (Zero-Copy)
df = lake.query("""
    SELECT ts_event, open, close
    FROM silver.klines
    WHERE symbol = 'BTCUSDT' AND interval = '1h'
""").pl()

# 3. Access Metadata
instruments = lake.registry.get_instruments(trade_type='um')
```

## 4. Implementation Strategy
- **Dependency Inversion**: The SDK will be independent of the ingestion toolkit (`binance-datatool`), depending only on `duckdb` and `huggingface_hub`.
- **DVC Integration**: Use `dvc.api.get_url()` or `huggingface_hub` to resolve the actual Parquet file locations from the HF repo.
- **Extension Management**: Bundled logic to handle `INSTALL ducklake; LOAD ducklake;` across different OS/environments.

## 5. Roadmap
- **v0.1**: Basic connection and schema mapping (manual HF download).
- **v0.2**: Automated `huggingface_hub` file resolution and caching.
- **v0.3**: Point-in-Time universe resolution helpers.
- **v0.4**: Integration with `vectorbt.pro` for direct backtesting ingestion.
