# Visualization Patterns

## Distribution Plots

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Histogram with KDE
sns.histplot(data=df, x='column', kde=True)

# Boxplot
sns.boxplot(data=df, x='category', y='value')
```

## Correlation Visualization

```python
# Heatmap
corr = df.corr()
sns.heatmap(corr, annot=True, cmap='coolwarm')
```

## Interactive with Plotly

```python
import plotly.express as px

fig = px.scatter(df, x='x', y='y', color='category')
fig.show()
```
