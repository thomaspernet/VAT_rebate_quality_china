# VAT Rebate & Quality Upgrading in China

Academic research studying the effect of China's **VAT export tax rebate** policy on product quality upgrading. Uses Chinese customs transaction data (2002–2006) to identify how industrial policy affects export quality.

## Abstract

This paper studies the effect of industrial policy in China — specifically the VAT export tax — on quality upgrading. We use Chinese transaction data for 2002–2006 to isolate the causal impact of exogenous variation in VAT refund rates and within firm-product changes in HS6 exported product quality.

### Research Design

- **Identification Strategy** — exploiting exogenous variation in VAT rebate rates across products and time
- **Unit of Analysis** — firm-product (HS6 code) level
- **Quality Measurement** — unit value-based quality proxies with controls for composition effects

## Tech Stack

- **Python** — Pandas, NumPy, Statsmodels, SciPy
- **AWS** — S3 (`datalake-london`), Glue (catalog), Athena (queries)
- **Jupyter Notebooks** — econometric analysis
- **LaTeX** — academic paper formatting

## Project Structure

```
VAT_rebate_quality_china/
├── 00_data_catalog/                  # Data catalog with HTML analysis reports
├── 01_data_preprocessing/
│   ├── 00_download_data/             # Chinese customs data ingestion
│   └── 01_transform_tables/          # Feature engineering (quality measures, VAT rates)
├── 02_data_analysis/
│   └── 01_model_train_evaluate/      # Econometric estimation
├── utils/
│   └── latex/                        # LaTeX formatting
└── creds/                            # AWS credentials (gitignored)
```

## Data Sources

- **Chinese Customs Data** — firm-product-level trade transactions (2002–2006)
- **VAT Rebate Rates** — product-level export tax rebate schedules
- **HS6 Product Codes** — Harmonized System 6-digit product classification

## Author

**Thomas Pernet-Coudrier**
