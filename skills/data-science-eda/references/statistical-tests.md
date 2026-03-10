# Statistical Tests Reference

## Normality Tests

```python
from scipy import stats

# Shapiro-Wilk (small samples)
stats.shapiro(sample)

# Anderson-Darling
stats.anderson(sample, dist='norm')

# Kolmogorov-Smirnov
stats.kstest(sample, 'norm')
```

## Correlation Tests

```python
# Pearson (linear, normal)
stats.pearsonr(x, y)

# Spearman (rank, monotonic)
stats.spearmanr(x, y)

# Kendall (rank, ordinal)
stats.kendalltau(x, y)
```

## Group Comparisons

```python
# T-test
stats.ttest_ind(group1, group2)

# ANOVA
stats.f_oneway(group1, group2, group3)

# Chi-square (categorical)
stats.chi2_contingency(contingency_table)
```
