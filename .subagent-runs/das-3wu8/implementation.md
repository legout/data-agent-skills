---
name: analyzing-data
description: "Exploratory Data Analysis (EDA), statistics, and data visualization: profiling datasets, statistical analysis, choosing appropriate charts, applying statistical tests, and creating effective visualizations for insight communication. Use when understanding data structure, exploring distributions and relationships, selecting visualization libraries, running statistical tests, or producing analysis-ready charts."
---

# Analyzing Data

Use this skill for exploratory data analysis, statistics, and visualization: understanding dataset structure, identifying patterns through statistical analysis, choosing the right visualization approach, and communicating insights effectively.

## When to use this skill

- New dataset — need orientation on structure, types, distributions
- Choosing visualization libraries and chart types for a project
- Data quality investigation — find anomalies, missing patterns, outliers
- Statistical hypothesis testing — validate assumptions about data
- Creating publication-quality figures or exploratory charts
- Large dataset exploration — sampling and aggregation strategies
- Understanding missing value mechanisms (MCAR/MAR/MNAR)
- Before feature engineering — understand variable relationships
- Model preparation — validate assumptions about data

## When NOT to use this skill

- Building interactive dashboards or data applications → use `building-data-apps`
- Feature engineering for ML pipelines → use `engineering-ml-features`
- Model evaluation and comparison → use `evaluating-ml-models`
- Notebook-specific workflows (Jupyter/marimo setup) → use `working-in-notebooks`

## Quick tool selection

| Task | Default choice | Notes |
|---|---|---|
| Automated profiling | **ydata-profiling / pandas-profiling** | Fast comprehensive reports |
| Statistical visualization | **Seaborn** | Quick EDA with statistical defaults |
| Publication-quality static plots | **Matplotlib** | Fine control over every element |
| Interactive web charts | **Plotly** | Easy interactive dashboards |
| Large datasets (100k+ points) | **hvPlot + Datashader** | Automatic rasterization |
| Declarative grammar | **Altair** | Vega-Lite transformations |
| Statistical tests | **scipy.stats** | Normality, correlations, t-tests |

## Core analysis workflow

1. **Profile structure**
   - Schema, types, cardinality
   - Missing value patterns
   - Automated profiling with ydata-profiling

2. **Analyze distributions**
   - Numerical: histograms, boxplots, KDE, skewness
   - Categorical: frequencies, rare categories
   - Identify outliers and anomalies

3. **Explore relationships**
   - Correlation matrix (numerical)
   - Cross-tabulations (categorical)
   - Target-variable relationships
   - Statistical significance tests

4. **Visualize for insight**
   - Match chart type to question
   - Maximize data-ink ratio
   - Choose appropriate interactivity level

5. **Validate and document**
   - Check assumptions against domain knowledge
   - Document findings for team
   - Flag issues for investigation

## Library selection guide

### Static visualization

| Library | Best For | Learning Curve | Interactivity |
|---|---|---|---|
| **Matplotlib** | Publication-quality plots, fine control | Moderate | Static |
| **Seaborn** | Statistical visualization, quick EDA | Easy | Static |

### Interactive visualization

| Library | Best For | Interactivity |
|---|---|---|
| **Plotly** | Web charts, dashboards | High |
| **Altair** | Declarative statistical charts, large datasets | Medium |
| **hvPlot/HoloViz** | Large data, linked brushing, geospatial | High |
| **Bokeh** | Custom interactive web apps | High |

### Statistical analysis

| Library | Best For |
|---|---|
| **scipy.stats** | Hypothesis tests, distributions |
| **statsmodels** | Regression diagnostics, time series |

## Quick decision tree

```
Static publication figure?
  → Matplotlib (full control) or Seaborn (quick statistical)

Interactive web/dashboard?
  → Plotly (easiest), Dash (full apps)
  → Panel/HoloViz (complex linked views)
  → Bokeh (custom web apps)

Large datasets (100k+ points)?
  → hvPlot + Datashader (automatic rasterization)
  → Altair (smart aggregation with Vega-Lite)

Declarative grammar preferred?
  → Altair (Vega-Lite) or Plotly Express

Already using Pandas?
  → df.plot() → Matplotlib
  → df.hvplot() → HoloViz
  → px.scatter(df) → Plotly
```

## Core implementation principles

### Match chart to question

| Question | Chart Type |
|---|---|
| Distribution? | Histogram, KDE, boxplot, violin |
| Relationship? | Scatter, line, heatmap |
| Comparison? | Bar, grouped bar, dot plot |
| Trend over time? | Line, area |
| Composition? | Stacked bar, treemap (avoid pie charts) |
| Geographic? | Choropleth, scatter map, heatmap |

### Maximize data-ink ratio

- Remove unnecessary gridlines, borders, backgrounds
- Use color purposefully (not decoration)
- Label directly when possible
- One message per visualization

### Validate assumptions

- Check for expected ranges/business rules
- Verify temporal consistency
- Confirm key relationships match domain knowledge
- Apply appropriate statistical tests

## Quick code examples

### Matplotlib (fine control)

```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(x, y, c=colors, alpha=0.6, edgecolors='none')
ax.set_xlabel('Feature X', fontsize=12)
ax.set_ylabel('Target Y', fontsize=12)
ax.set_title('Relationship Analysis', fontsize=14, fontweight='bold')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
```

### Seaborn (statistical)

```python
import seaborn as sns

# Distribution with KDE
sns.histplot(data=df, x='value', hue='category', kde=True, bins=30)

# Correlation heatmap
corr = df.corr()
sns.heatmap(corr, annot=True, fmt='.2f', cmap='coolwarm', center=0)

# Categorical comparison
sns.boxplot(data=df, x='category', y='value', palette='viridis')
```

### Plotly (interactive web)

```python
import plotly.express as px

# Scatter with marginal distributions
fig = px.scatter(df, x='x', y='y', color='category', size='size',
                 marginal_x='histogram', marginal_y='rug',
                 hover_data=['label'])
fig.show()

# Faceted small multiples
fig = px.line(df, x='date', y='value', facet_col='category',
              facet_col_wrap=3, height=800)
fig.show()
```

### Altair (declarative, large data)

```python
import altair as alt

# Smart aggregation for large datasets
chart = alt.Chart(df).mark_circle().encode(
    x=alt.X('x:Q', bin=alt.Bin(maxbins=50)),
    y=alt.Y('y:Q', bin=alt.Bin(maxbins=50)),
    size='count()'
).interactive()

chart.save('chart.html')  # Self-contained HTML
```

### hvPlot/HoloViz (large data, linked views)

```python
import hvplot.pandas
import panel as pn

# Linked brushing
scatter = df.hvplot.scatter(x='x', y='y', c='category', 
                            tools=['box_select'], 
                            width=400, height=400)
hist = df.hvplot.hist(y='y', width=400, height=200)

layout = pn.Row(scatter, hist)
layout.servable()
```

### Bokeh (custom web apps)

```python
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool

source = ColumnDataSource(df)

p = figure(title="Interactive Plot", tools="pan,wheel_zoom,box_select")
p.circle('x', 'y', source=source, size=10, alpha=0.6)

hover = HoverTool(tooltips=[("X", "@x"), ("Y", "@y"), ("Label", "@label")])
p.add_tools(hover)

show(p)
```

### Automated profiling

```python
import polars as pl
from ydata_profiling import ProfileReport

df = pl.read_parquet("data.parquet")
profile = ProfileReport(df.to_pandas(), title="Data Profile")
profile.to_file("profile_report.html")
```

## Common anti-patterns

- ❌ Skipping profiling and jumping to modeling
- ❌ Treating all outliers as errors (some are valid signals)
- ❌ Ignoring missing value mechanisms (MCAR/MAR/MNAR)
- ❌ Pie charts with many slices (use bar charts instead)
- ❌ Dual y-axes (hard to read, try normalization)
- ❌ 3D charts (distorts perception)
- ❌ Rainbow colormaps (use perceptually uniform: viridis, plasma)
- ❌ Overplotting large datasets without handling
- ❌ Not documenting findings for team

## Common issues and solutions

| Problem | Solution |
|---|---|
| Overplotting (100k+ points) | Use Datashader (rasterization), hexbin, or 2D histogram |
| Slow interactivity | Reduce data points, use WebGL (Plotly), or pre-aggregate |
| Large file size | Save as JSON (Plotly/Altair) or use static images |
| Color blindness | Use colorblind-friendly palettes (viridis, colorbrewer) |

## Choose interactivity appropriately

| Audience | Interactivity Level |
|---|---|
| Paper/report | Static (Matplotlib/Seaborn) |
| Presentation | Limited (Plotly static export) |
| Exploratory analysis | High (zoom, pan, filter, hover) |
| Stakeholder dashboard | Medium (linked views, drill-down) |

## Progressive disclosure

- `references/profiling-automation.md` — ydata-profiling, Sweetviz, D-Tale automated profiling
- `references/statistical-tests.md` — SciPy and statsmodels statistical testing guide
- `references/visualization-libraries.md` — Matplotlib, Seaborn, Plotly, Altair, HoloViz, Bokeh patterns
- `references/large-dataset-eda.md` — Sampling, aggregation, Datashader for large data
- `references/matplotlib-advanced.md` — Subplots, annotations, custom styles
- `references/seaborn-statistical.md` — Complex statistical plots
- `references/plotly-dash.md` — Full dashboards with callbacks
- `references/altair-grammar.md` — Vega-Lite transformations
- `references/holoviz-datashader.md` — Large data visualization
- `references/bokeh-server.md` — Real-time streaming apps

## Related skills

- `engineering-ml-features` — Next step: transform insights into model features
- `evaluating-ml-models` — Validate modeling assumptions with proper evaluation
- `building-data-apps` — Build interactive dashboards from analysis results
- `working-in-notebooks` — Notebook-specific workflows and reproducibility

## References

- [ydata-profiling Documentation](https://docs.profiling.ydata.ai/)
- [Matplotlib Documentation](https://matplotlib.org/)
- [Seaborn Documentation](https://seaborn.pydata.org/)
- [Plotly Python](https://plotly.com/python/)
- [Altair Documentation](https://altair-viz.github.io/)
- [HoloViz Tutorial](https://holoviz.org/tutorial/)
- [Bokeh Documentation](https://docs.bokeh.org/)
- [SciPy Statistics](https://docs.scipy.org/doc/scipy/reference/stats.html)
- [Python Graph Gallery](https://python-graph-gallery.com/) (examples by chart type)
- [Pandas Visualization](https://pandas.pydata.org/docs/user_guide/visualization.html)
