#!/bin/bash
# scripts/setup_local.sh — One-shot local dev setup
set -e

echo "╔══════════════════════════════════════╗"
echo "║  EconPulse — Local Setup             ║"
echo "╚══════════════════════════════════════╝"

# ── 1. Check prerequisites ────────────────────────────
echo ""
echo "[ 1/6 ] Checking prerequisites..."
command -v R        >/dev/null || { echo "❌  R not found. Install from https://cran.r-project.org"; exit 1; }
command -v psql     >/dev/null || { echo "❌  PostgreSQL not found. Install PostgreSQL 14+"; exit 1; }
command -v python3  >/dev/null || { echo "❌  Python3 not found"; exit 1; }
echo "      ✅  R, psql, python3 found"

# ── 2. Create .env if missing ─────────────────────────
echo "[ 2/6 ] Setting up environment variables..."
if [ ! -f .env ]; then
  cp .env.example .env
  echo "      ✅  Created .env from .env.example (edit credentials!)"
else
  echo "      ✅  .env already exists"
fi
export $(grep -v '^#' .env | xargs)

# ── 3. Create PostgreSQL database ─────────────────────
echo "[ 3/6 ] Setting up PostgreSQL..."
DB_NAME=${DB_NAME:-economicdb}
DB_USER=${DB_USER:-postgres}

psql -U "$DB_USER" -tc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 || \
  psql -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"

psql -U "$DB_USER" -d "$DB_NAME" -f sql/schema/star_schema.sql
echo "      ✅  Schema applied"

# ── 4. Install Python packages ────────────────────────
echo "[ 4/6 ] Installing Python packages..."
pip3 install psycopg2-binary python-dotenv requests --quiet
echo "      ✅  Python packages installed"

# ── 5. Seed database ──────────────────────────────────
echo "[ 5/6 ] Seeding database with economic data..."
python3 scripts/seed_data.py
echo "      ✅  Database seeded"

# ── 6. Install R packages ─────────────────────────────
echo "[ 6/6 ] Installing R packages..."
Rscript -e "
  pkgs <- c('shiny','DBI','RPostgres','dplyr','jsonlite','httr','plotly','lubridate','tidyr')
  missing <- pkgs[!(pkgs %in% installed.packages()[,'Package'])]
  if (length(missing)) install.packages(missing, repos='https://cran.rstudio.com/', quiet=TRUE)
  cat('R packages ready\n')
"
echo "      ✅  R packages installed"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Setup complete! 🎉                              ║"
echo "║                                                  ║"
echo "║  Start dashboard:                                ║"
echo "║  cd shiny-app && Rscript -e                      ║"
echo "║  \"shiny::runApp('.', port=3838)\"                  ║"
echo "║                                                  ║"
echo "║  Then open: http://localhost:3838                ║"
echo "╚══════════════════════════════════════════════════╝"
