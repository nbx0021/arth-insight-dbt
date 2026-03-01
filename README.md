# Arth-Insight: dbt Transformation Layer

This repository contains the **dbt (Data Build Tool)** project for the Arth-Insight Fintech platform. It is responsible for transforming raw market data from BigQuery into production-ready analytical models.

## 🏗 Data Pipeline Architecture

1.  **Sources (`stg_stocks`):** Standardizes raw hourly data fetched from Yahoo Finance.
2.  **Transformations (`fact_stock_analysis`):** Implements the **V3 Scoring Engine** (Sector-aware health scores).
3.  **Exposures:** Serves the "Gold" layer to the Django web application hosted on Render.

## 🚀 Key Data Models

- **`fact_stock_analysis`**: The core analytical model. It calculates the **Arth-Score (0-100)** based on:
    - ROE & Profit Growth (Sector-adjusted).
    - Debt-to-Equity thresholds (Capital-intensive vs. Service-based).
    - Promoter Holding & Market Cap stability.
    - Technical indicators (RSI, Moving Averages).

## 🔧 Configuration & Setup

### Environment Variables
To run this project locally or in dbt Cloud, you must define:
- `GCP_PROJECT_ID`: Your Google Cloud Project ID.
- `GCP_DATASET_ID`: The dataset containing raw stock data.

### Running the Project
```bash
# Install dependencies
dbt deps

# Run models and tests
dbt build

```

## 🕵️‍♂️ Quality Control

* **Source Freshness:** Monitors `stock_intelligence_v3` to ensure GitHub Actions is delivering fresh data hourly.
* **Data Tests:** Ensures `ticker` is not null and `price` is always positive before data reaches the production dashboard.

---

**Author:** Narendra Bhandari
*Data Engineer - Arth-Insight Project*
