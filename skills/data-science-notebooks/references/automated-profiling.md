# Automated Profiling Tools

## ydata-profiling (formerly pandas-profiling)

Generates comprehensive HTML reports with:
- Type inference and warnings
- Univariate distributions
- Correlations
- Missing value analysis
- Duplicate rows

```python
from ydata_profiling import ProfileReport

profile = ProfileReport(df, title="Profiling Report")
profile.to_file("report.html")
```

## Sweetviz

Side-by-side dataset comparison ideal for train vs test analysis.

```python
import sweetviz as sv

report = sv.compare([train, "Train"], [test, "Test"])
report.show_html()
```

## D-Tale

Interactive browser-based exploration.

```python
import dtale

d = dtale.show(df)
d.open_browser()
```
