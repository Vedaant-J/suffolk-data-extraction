# Suffolk Data Extraction

Pulls real data distributions from Suffolk's Databricks warehouse for synthetic data generation.

## Setup

Run in the argo environment where kumo imports and AWS secrets are available.

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Run all scripts
python run_all.py

# Or run individually
python 01_basic_stats.py
python 02_categorical_distributions.py
python 03_numerical_distributions.py
python 04_incident_correlations.py
python 05_sample_rows.py
```

## Scripts

| Script | What it does | Output |
|--------|-------------|--------|
| `01_basic_stats.py` | Row counts, column schemas, date ranges, project match rates | `output/basic_stats.json` |
| `02_categorical_distributions.py` | Value frequencies for all categorical/boolean columns | `output/categorical_distributions.json` |
| `03_numerical_distributions.py` | Min/max/mean/std/percentiles for all numerical columns | `output/numerical_distributions.json` |
| `04_incident_correlations.py` | 15 correlation queries linking incidents to worker density, trades, observations, schedule pressure, etc. | `output/incident_correlations.json` |
| `05_sample_rows.py` | Sample rows from all tables + deep samples for top-incident projects | `output/sample_rows.json` |

## Output

All outputs go to `output/` (gitignored). JSON files that can be used to parameterize a synthetic data generator.
