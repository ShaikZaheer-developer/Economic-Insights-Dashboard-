# 🌐 Economic Insights Dashboard

> **Enterprise-grade economic analytics platform** — Real-time data pipeline, AWS Data Lake, R Shiny backend, and an interactive dashboard with live KPIs, sector heatmaps, and Google Maps integration.

![Dashboard Preview](docs/preview.png)

---

## 🚀 Live Demo Architecture

```
Raw Data Sources (World Bank API / FRED / Yahoo Finance)
        │
        ▼
   AWS S3 Data Lake (Raw Zone)
        │
   AWS Glue ETL ──► Apache Spark (EMR)
        │
   AWS Redshift (Star Schema DWH)
        │
   R Shiny API Layer  ◄──► PostgreSQL (local dev)
        │
   HTML5 Dashboard (Frontend)
        │
   Google Maps API (Geo Layer)
```

---

## 📁 Project Structure

```
economic-insights-dashboard/
├── shiny-app/                  # R Shiny backend
│   ├── app.R                   # Main Shiny app entry
│   ├── R/
│   │   ├── data_fetch.R        # API + DB data fetching
│   │   ├── analytics.R         # KPI computation
│   │   ├── charts.R            # Plotly chart builders
│   │   └── utils.R             # Helper functions
│   └── www/                    # Static assets served by Shiny
│       ├── index.html          # Main dashboard UI
│       ├── css/dashboard.css   # Styling
│       └── js/dashboard.js     # Frontend logic
├── sql/
│   ├── schema/
│   │   ├── star_schema.sql     # Redshift star schema
│   │   └── local_postgres.sql  # Local dev schema
│   ├── queries/
│   │   ├── gdp_analysis.sql
│   │   ├── sector_performance.sql
│   │   └── regional_metrics.sql
│   └── views/
│       └── reporting_views.sql
├── aws/
│   ├── glue/etl_job.py         # AWS Glue Spark ETL
│   ├── lambda/
│   │   ├── trigger_pipeline.py # S3 event trigger
│   │   └── data_quality.py     # Data validation
│   └── emr/spark_job.py        # EMR Spark processing
├── scripts/
│   ├── setup_local.sh          # Local dev setup
│   ├── deploy_aws.sh           # AWS deployment
│   └── seed_data.py            # Sample data seeder
├── data/sample/                # Sample CSV data
└── docs/                       # Documentation
```

---

## ⚡ Quick Start (Local Development)

### Prerequisites
```bash
# R 4.3+
# PostgreSQL 14+
# Python 3.10+
# Node.js 18+
```

### 1. Clone & Setup
```bash
git clone https://github.com/YOUR_USERNAME/economic-insights-dashboard.git
cd economic-insights-dashboard
chmod +x scripts/setup_local.sh
./scripts/setup_local.sh
```

### 2. Database Setup
```bash
psql -U postgres -f sql/schema/local_postgres.sql
python scripts/seed_data.py
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 4. Launch Shiny App
```bash
cd shiny-app
Rscript -e "shiny::runApp('.', port=3838, host='0.0.0.0')"
```

Open `http://localhost:3838` → Dashboard loads ✅

---

## 🏗️ AWS Architecture Setup

### Step 1: S3 Data Lake
```bash
aws s3 mb s3://economic-insights-raw
aws s3 mb s3://economic-insights-processed
aws s3 mb s3://economic-insights-scripts
```

### Step 2: Deploy Glue ETL
```bash
aws s3 cp aws/glue/etl_job.py s3://economic-insights-scripts/
aws glue create-job --name economic-etl --cli-input-json file://aws/glue/job_config.json
```

### Step 3: Setup Redshift
```bash
# Create cluster via AWS Console or:
aws redshift create-cluster \
  --cluster-identifier economic-insights \
  --node-type dc2.large \
  --number-of-nodes 2 \
  --master-username admin \
  --master-user-password YOUR_PASSWORD \
  --db-name economicdb
```

### Step 4: Lambda Triggers
```bash
cd aws/lambda
zip trigger_pipeline.zip trigger_pipeline.py
aws lambda create-function \
  --function-name economic-pipeline-trigger \
  --zip-file fileb://trigger_pipeline.zip \
  --handler trigger_pipeline.lambda_handler \
  --runtime python3.10 \
  --role arn:aws:iam::ACCOUNT_ID:role/LambdaRole
```

---

## 📊 Data Sources

| Source | Type | Refresh Rate |
|--------|------|-------------|
| World Bank API | GDP, Inflation, Trade | Daily |
| FRED (Federal Reserve) | Interest Rates, CPI | Real-time |
| Yahoo Finance | Stock Indices | Live |
| UN Comtrade | Trade Flows | Monthly |
| OpenStreetMap | Geographic | Static |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | HTML5, CSS3, Vanilla JS, Chart.js, Google Maps API |
| **Backend** | R Shiny, Plumber API |
| **Database (Dev)** | PostgreSQL 14 |
| **Database (Prod)** | AWS Redshift |
| **ETL** | AWS Glue + Apache Spark |
| **Compute** | AWS EMR |
| **Orchestration** | AWS Lambda + EventBridge |
| **Storage** | AWS S3 |
| **Language (ETL)** | Python, PySpark, HiveQL |

---

## 🔑 Environment Variables

```env
# .env.example
DB_HOST=localhost
DB_PORT=5432
DB_NAME=economicdb
DB_USER=postgres
DB_PASSWORD=your_password

# AWS (for production)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
REDSHIFT_HOST=your-cluster.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=economicdb

# APIs
GOOGLE_MAPS_API_KEY=your_gmaps_key
FRED_API_KEY=your_fred_key
WORLD_BANK_API_KEY=optional
```

---

## 📈 Features

- ✅ **Real-time KPI Cards** — GDP, Inflation, Unemployment, Trade Balance
- ✅ **Interactive Line Charts** — Multi-country GDP trend comparison  
- ✅ **Sector Heatmap** — Performance across 11 GICS sectors
- ✅ **Google Maps Integration** — Economic indicators by geography
- ✅ **Live Data Pipeline** — AWS S3 → Glue → Redshift
- ✅ **Star Schema DWH** — Optimized for analytical queries
- ✅ **Auto-refresh** — 30-second polling for live updates
- ✅ **Dark/Light Mode** — Professional UI toggle
- ✅ **Export** — CSV/PDF report generation
- ✅ **Responsive** — Mobile + Desktop

---

## 👤 Author

**Your Name**  
Data Engineer & Analytics Developer  
[LinkedIn](https://linkedin.com) · [GitHub](https://github.com)

---

*Built as a showcase of end-to-end data engineering: from raw API ingestion to production-grade analytics dashboards.*
