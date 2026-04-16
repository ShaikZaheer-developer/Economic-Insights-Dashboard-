-- ============================================================
-- STAR SCHEMA: Economic Insights Data Warehouse
-- Compatible: AWS Redshift + PostgreSQL
-- ============================================================

-- ─────────────────────────────────────────────
-- DIMENSION TABLES
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT PRIMARY KEY,          -- YYYYMMDD
    full_date       DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,        -- 1–4
    month           SMALLINT NOT NULL,        -- 1–12
    month_name      VARCHAR(12),
    week            SMALLINT,
    day_of_month    SMALLINT,
    day_name        VARCHAR(12),
    is_weekend      BOOLEAN DEFAULT FALSE,
    fiscal_year     SMALLINT,
    fiscal_quarter  SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_key     SERIAL PRIMARY KEY,
    country_code    CHAR(3) NOT NULL UNIQUE,  -- ISO 3166-1 alpha-3
    country_name    VARCHAR(100) NOT NULL,
    region          VARCHAR(60),              -- e.g. "South Asia"
    sub_region      VARCHAR(60),
    income_group    VARCHAR(40),              -- World Bank classification
    currency_code   CHAR(3),
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    population      BIGINT,
    area_sq_km      BIGINT,
    is_g20          BOOLEAN DEFAULT FALSE,
    is_eu           BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_key   SERIAL PRIMARY KEY,
    indicator_code  VARCHAR(60) NOT NULL UNIQUE,  -- e.g. "NY.GDP.MKTP.CD"
    indicator_name  VARCHAR(200) NOT NULL,
    category        VARCHAR(80),                  -- e.g. "Macroeconomic"
    sub_category    VARCHAR(80),                  -- e.g. "Output & Growth"
    unit            VARCHAR(40),                  -- e.g. "USD Billion", "%"
    source          VARCHAR(100),                 -- e.g. "World Bank"
    frequency       VARCHAR(20),                  -- "Annual", "Quarterly", "Monthly"
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_sector (
    sector_key      SERIAL PRIMARY KEY,
    sector_code     VARCHAR(20) NOT NULL UNIQUE,
    sector_name     VARCHAR(100) NOT NULL,
    gics_code       VARCHAR(10),                  -- GICS standard
    description     TEXT
);

CREATE TABLE IF NOT EXISTS dim_source (
    source_key      SERIAL PRIMARY KEY,
    source_name     VARCHAR(100) NOT NULL,
    source_url      VARCHAR(255),
    api_endpoint    VARCHAR(255),
    reliability     SMALLINT DEFAULT 5,           -- 1-10 score
    last_updated    TIMESTAMP
);

-- ─────────────────────────────────────────────
-- FACT TABLES
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_economic_indicators (
    indicator_id    BIGSERIAL PRIMARY KEY,
    date_key        INT REFERENCES dim_date(date_key),
    country_key     INT REFERENCES dim_country(country_key),
    indicator_key   INT REFERENCES dim_indicator(indicator_key),
    source_key      INT REFERENCES dim_source(source_key),
    value           DECIMAL(20, 6),
    yoy_change      DECIMAL(10, 4),               -- Year-over-year %
    qoq_change      DECIMAL(10, 4),               -- Quarter-over-quarter %
    mom_change      DECIMAL(10, 4),               -- Month-over-month %
    is_estimated    BOOLEAN DEFAULT FALSE,
    is_revised      BOOLEAN DEFAULT FALSE,
    load_timestamp  TIMESTAMP DEFAULT NOW(),
    pipeline_run_id VARCHAR(60)
) DISTKEY(country_key) SORTKEY(date_key, country_key);

CREATE TABLE IF NOT EXISTS fact_trade_flows (
    trade_id        BIGSERIAL PRIMARY KEY,
    date_key        INT REFERENCES dim_date(date_key),
    exporter_key    INT REFERENCES dim_country(country_key),
    importer_key    INT REFERENCES dim_country(country_key),
    sector_key      INT REFERENCES dim_sector(sector_key),
    export_value_usd DECIMAL(20, 2),
    import_value_usd DECIMAL(20, 2),
    trade_balance   DECIMAL(20, 2),
    yoy_growth      DECIMAL(10, 4),
    load_timestamp  TIMESTAMP DEFAULT NOW()
) DISTKEY(exporter_key) SORTKEY(date_key);

CREATE TABLE IF NOT EXISTS fact_market_data (
    market_id       BIGSERIAL PRIMARY KEY,
    date_key        INT REFERENCES dim_date(date_key),
    country_key     INT REFERENCES dim_country(country_key),
    sector_key      INT REFERENCES dim_sector(sector_key),
    index_name      VARCHAR(80),
    open_price      DECIMAL(12, 4),
    close_price     DECIMAL(12, 4),
    high_price      DECIMAL(12, 4),
    low_price       DECIMAL(12, 4),
    volume          BIGINT,
    market_cap_usd  DECIMAL(20, 2),
    pe_ratio        DECIMAL(10, 4),
    load_timestamp  TIMESTAMP DEFAULT NOW()
) DISTKEY(country_key) SORTKEY(date_key);

-- ─────────────────────────────────────────────
-- STAGING TABLES (for ETL loads)
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stg_raw_indicators (
    raw_id          BIGSERIAL PRIMARY KEY,
    source          VARCHAR(100),
    country_code    CHAR(3),
    indicator_code  VARCHAR(60),
    period          VARCHAR(10),                  -- "2023" or "2023-Q1" or "2023-01"
    raw_value       VARCHAR(100),                 -- raw string before casting
    unit            VARCHAR(40),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE,
    error_message   TEXT
);

-- ─────────────────────────────────────────────
-- INDEXES (PostgreSQL dev — Redshift uses SORTKEY/DISTKEY)
-- ─────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_econ_date     ON fact_economic_indicators(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_econ_country  ON fact_economic_indicators(country_key);
CREATE INDEX IF NOT EXISTS idx_fact_econ_indicator ON fact_economic_indicators(indicator_key);
CREATE INDEX IF NOT EXISTS idx_country_code       ON dim_country(country_code);
CREATE INDEX IF NOT EXISTS idx_indicator_code     ON dim_indicator(indicator_code);
CREATE INDEX IF NOT EXISTS idx_date_full          ON dim_date(full_date);

-- ─────────────────────────────────────────────
-- SEED: dim_date (2015–2030)
-- ─────────────────────────────────────────────

INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, week, day_of_month, day_name, is_weekend, fiscal_year, fiscal_quarter)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TO_CHAR(d, 'Month'),
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(DAY FROM d)::SMALLINT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DOW FROM d) IN (0, 6),
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT
FROM generate_series('2015-01-01'::DATE, '2030-12-31'::DATE, '1 day') d
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────────
-- SEED: dim_country
-- ─────────────────────────────────────────────

INSERT INTO dim_country (country_code, country_name, region, sub_region, income_group, currency_code, latitude, longitude, is_g20) VALUES
('USA', 'United States',       'Americas',    'Northern America',  'High income',         'USD',  37.09,   -95.71, TRUE),
('CHN', 'China',               'Asia',        'Eastern Asia',      'Upper middle income', 'CNY',  35.86,   104.19, TRUE),
('IND', 'India',               'Asia',        'Southern Asia',     'Lower middle income', 'INR',  20.59,   78.96,  TRUE),
('DEU', 'Germany',             'Europe',      'Western Europe',    'High income',         'EUR',  51.17,   10.45,  TRUE),
('GBR', 'United Kingdom',      'Europe',      'Northern Europe',   'High income',         'GBP',  55.38,   -3.44,  TRUE),
('JPN', 'Japan',               'Asia',        'Eastern Asia',      'High income',         'JPY',  36.20,   138.25, TRUE),
('FRA', 'France',              'Europe',      'Western Europe',    'High income',         'EUR',  46.23,   2.21,   TRUE),
('BRA', 'Brazil',              'Americas',    'South America',     'Upper middle income', 'BRL', -14.24,  -51.93,  TRUE),
('CAN', 'Canada',              'Americas',    'Northern America',  'High income',         'CAD',  56.13,  -106.35, TRUE),
('AUS', 'Australia',           'Oceania',     'Australia/NZ',      'High income',         'AUD', -25.27,  133.78,  TRUE),
('KOR', 'South Korea',         'Asia',        'Eastern Asia',      'High income',         'KRW',  35.91,   127.77, TRUE),
('MEX', 'Mexico',              'Americas',    'Central America',   'Upper middle income', 'MXN',  23.63,  -102.55, TRUE),
('IDN', 'Indonesia',           'Asia',        'South-eastern Asia','Lower middle income', 'IDR',  -0.79,  113.92,  TRUE),
('SAU', 'Saudi Arabia',        'Asia',        'Western Asia',      'High income',         'SAR',  23.88,   45.08,  TRUE),
('ZAF', 'South Africa',        'Africa',      'Sub-Saharan Africa','Upper middle income', 'ZAR', -30.56,   22.94,  TRUE),
('RUS', 'Russia',              'Europe',      'Eastern Europe',    'Upper middle income', 'RUB',  61.52,   105.32, TRUE),
('TUR', 'Türkiye',             'Asia',        'Western Asia',      'Upper middle income', 'TRY',  38.96,   35.24,  TRUE),
('ARG', 'Argentina',           'Americas',    'South America',     'Upper middle income', 'ARS', -38.42,  -63.62,  TRUE),
('SGP', 'Singapore',           'Asia',        'South-eastern Asia','High income',         'SGD',   1.35,   103.82, FALSE),
('NLD', 'Netherlands',         'Europe',      'Western Europe',    'High income',         'EUR',  52.13,   5.29,   FALSE)
ON CONFLICT (country_code) DO NOTHING;

-- ─────────────────────────────────────────────
-- SEED: dim_indicator
-- ─────────────────────────────────────────────

INSERT INTO dim_indicator (indicator_code, indicator_name, category, sub_category, unit, source, frequency) VALUES
('NY.GDP.MKTP.CD',     'GDP (current USD)',                  'Macroeconomic', 'Output & Growth',   'USD Billion',  'World Bank', 'Annual'),
('NY.GDP.MKTP.KD.ZG',  'GDP Growth Rate',                   'Macroeconomic', 'Output & Growth',   '%',            'World Bank', 'Annual'),
('FP.CPI.TOTL.ZG',     'Inflation (CPI)',                    'Macroeconomic', 'Prices',            '%',            'World Bank', 'Annual'),
('SL.UEM.TOTL.ZS',     'Unemployment Rate',                 'Labor',         'Employment',        '%',            'World Bank', 'Annual'),
('BN.CAB.XOKA.CD',     'Current Account Balance',           'Trade',         'Balance of Payments','USD Billion', 'World Bank', 'Annual'),
('NE.EXP.GNFS.ZS',     'Exports (% of GDP)',                'Trade',         'External Sector',   '%',            'World Bank', 'Annual'),
('NE.IMP.GNFS.ZS',     'Imports (% of GDP)',                'Trade',         'External Sector',   '%',            'World Bank', 'Annual'),
('GC.DOD.TOTL.GD.ZS',  'Government Debt (% of GDP)',        'Fiscal',        'Public Finance',    '%',            'World Bank', 'Annual'),
('FR.INR.RINR',         'Real Interest Rate',                'Monetary',      'Interest Rates',    '%',            'World Bank', 'Annual'),
('SP.POP.TOTL',         'Total Population',                  'Demographic',   'Population',        'Millions',     'World Bank', 'Annual'),
('NY.GNP.PCAP.CD',     'GNI per Capita',                    'Macroeconomic', 'Income',            'USD',          'World Bank', 'Annual'),
('EG.USE.PCAP.KG.OE',  'Energy Use per Capita',             'Environment',   'Energy',            'kg oil equiv', 'World Bank', 'Annual'),
('IT.NET.USER.ZS',     'Internet Users (% of population)',  'Technology',    'Digital',           '%',            'World Bank', 'Annual'),
('SE.XPD.TOTL.GD.ZS',  'Education Spending (% of GDP)',     'Social',        'Education',         '%',            'World Bank', 'Annual')
ON CONFLICT (indicator_code) DO NOTHING;

-- ─────────────────────────────────────────────
-- SEED: dim_sector (GICS)
-- ─────────────────────────────────────────────

INSERT INTO dim_sector (sector_code, sector_name, gics_code) VALUES
('ENERGY',    'Energy',                  '10'),
('MATERIALS', 'Materials',               '15'),
('INDUSTRIAL','Industrials',             '20'),
('CONS_DISC', 'Consumer Discretionary',  '25'),
('CONS_STAP', 'Consumer Staples',        '30'),
('HEALTH',    'Health Care',             '35'),
('FINANCIAL', 'Financials',              '40'),
('IT',        'Information Technology',  '45'),
('COMMS',     'Communication Services',  '50'),
('UTILITIES', 'Utilities',               '55'),
('REAL_EST',  'Real Estate',             '60')
ON CONFLICT (sector_code) DO NOTHING;
