# Large Dataset EDA

## Sampling Strategies

```python
import polars as pl

# Random sample
sample = df.sample(n=100_000, seed=42)

# Stratified sample
df.group_by('category').apply(lambda g: g.sample(n=min(1000, len(g))))
```

## Progressive Computation

```python
# Lazy evaluation
lazy_df = pl.scan_parquet('huge/*.parquet')
profile = lazy_df.describe()
```

## Dask for Out-of-Core

```python
import dask.dataframe as dd

ddf = dd.read_parquet('huge/*.parquet')
profile = ddf.describe().compute()
```
