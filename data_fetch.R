# R/data_fetch.R
# ─────────────────────────────────────────────
# DB query functions + fallback mock data
# ─────────────────────────────────────────────

# ── LIVE: KPI Summary ────────────────────────
fetch_kpi_summary <- function(conn) {
  sql <- "
    WITH latest AS (
      SELECT
        i.indicator_code,
        f.value,
        f.yoy_change,
        ROW_NUMBER() OVER (PARTITION BY i.indicator_code ORDER BY d.full_date DESC) AS rn
      FROM fact_economic_indicators f
      JOIN dim_date      d ON f.date_key      = d.date_key
      JOIN dim_country   c ON f.country_key   = c.country_key
      JOIN dim_indicator i ON f.indicator_key = i.indicator_key
      WHERE c.country_code = 'USA'
        AND i.indicator_code IN (
            'NY.GDP.MKTP.CD','NY.GDP.MKTP.KD.ZG',
            'FP.CPI.TOTL.ZG','SL.UEM.TOTL.ZS','BN.CAB.XOKA.CD'
        )
    )
    SELECT indicator_code, value, yoy_change FROM latest WHERE rn = 1
  "
  tryCatch({
    rows <- DBI::dbGetQuery(conn, sql)
    kpis <- list(
      gdp         = get_val(rows, "NY.GDP.MKTP.CD"),
      gdp_growth  = get_val(rows, "NY.GDP.MKTP.KD.ZG"),
      inflation   = get_val(rows, "FP.CPI.TOTL.ZG"),
      unemployment= get_val(rows, "SL.UEM.TOTL.ZS"),
      trade_balance = get_val(rows, "BN.CAB.XOKA.CD"),
      last_updated = format(Sys.time(), "%Y-%m-%d %H:%M UTC")
    )
    kpis
  }, error = function(e) {
    message("KPI fetch error: ", e$message)
    mock_kpi_data()
  })
}

get_val <- function(df, code) {
  row <- df[df$indicator_code == code, ]
  if (nrow(row) == 0) return(list(value=NA, change=NA))
  list(value = row$value[1], change = row$yoy_change[1])
}


# ── LIVE: GDP Trend ───────────────────────────
fetch_gdp_trend <- function(conn, year_start = 2015, countries = c('USA','CHN','IND','DEU','GBR')) {
  country_str <- paste0("'", paste(countries, collapse="','"), "'")
  sql <- sprintf("
    SELECT
      c.country_code, c.country_name,
      d.year,
      f.value AS gdp,
      f.yoy_change AS growth
    FROM fact_economic_indicators f
    JOIN dim_date      d ON f.date_key      = d.date_key
    JOIN dim_country   c ON f.country_key   = c.country_key
    JOIN dim_indicator i ON f.indicator_key = i.indicator_key
    WHERE i.indicator_code = 'NY.GDP.MKTP.CD'
      AND c.country_code IN (%s)
      AND d.year >= %d
      AND f.value IS NOT NULL
    ORDER BY c.country_code, d.year
  ", country_str, year_start)

  tryCatch(DBI::dbGetQuery(conn, sql), error = function(e) mock_gdp_trend())
}


# ── LIVE: Sector Heatmap ──────────────────────
fetch_sector_heatmap <- function(conn) {
  sql <- "
    SELECT
      s.sector_name,
      d.year, d.quarter,
      AVG(m.close_price / NULLIF(m.open_price,0) - 1) * 100 AS return_pct,
      AVG(m.pe_ratio) AS pe_ratio
    FROM fact_market_data m
    JOIN dim_date   d ON m.date_key   = d.date_key
    JOIN dim_sector s ON m.sector_key = s.sector_key
    WHERE d.year >= 2020
    GROUP BY s.sector_name, d.year, d.quarter
    ORDER BY d.year DESC, d.quarter DESC
  "
  tryCatch(DBI::dbGetQuery(conn, sql), error = function(e) mock_sector_data())
}


# ── LIVE: Country Map Data ────────────────────
fetch_country_map_data <- function(conn) {
  sql <- "
    SELECT
      c.country_name, c.country_code,
      c.latitude, c.longitude,
      c.region,
      MAX(CASE WHEN i.indicator_code='NY.GDP.MKTP.CD'    THEN f.value END) AS gdp,
      MAX(CASE WHEN i.indicator_code='NY.GDP.MKTP.KD.ZG' THEN f.value END) AS gdp_growth,
      MAX(CASE WHEN i.indicator_code='FP.CPI.TOTL.ZG'    THEN f.value END) AS inflation,
      MAX(CASE WHEN i.indicator_code='SL.UEM.TOTL.ZS'    THEN f.value END) AS unemployment
    FROM fact_economic_indicators f
    JOIN dim_date      d ON f.date_key      = d.date_key
    JOIN dim_country   c ON f.country_key   = c.country_key
    JOIN dim_indicator i ON f.indicator_key = i.indicator_key
    WHERE d.year = (SELECT MAX(year) FROM dim_date WHERE full_date <= NOW())
      AND i.indicator_code IN ('NY.GDP.MKTP.CD','NY.GDP.MKTP.KD.ZG','FP.CPI.TOTL.ZG','SL.UEM.TOTL.ZS')
    GROUP BY c.country_name, c.country_code, c.latitude, c.longitude, c.region
  "
  tryCatch(DBI::dbGetQuery(conn, sql), error = function(e) mock_country_map_data())
}


# ── LIVE: Trade Flows ─────────────────────────
fetch_trade_flows <- function(conn) {
  sql <- "
    SELECT
      ce.country_name AS exporter, ci.country_name AS importer,
      s.sector_name,
      d.year,
      SUM(t.export_value_usd)/1e9 AS export_bn,
      SUM(t.import_value_usd)/1e9 AS import_bn,
      AVG(t.yoy_growth) AS growth_pct
    FROM fact_trade_flows t
    JOIN dim_date    d  ON t.date_key     = d.date_key
    JOIN dim_country ce ON t.exporter_key = ce.country_key
    JOIN dim_country ci ON t.importer_key = ci.country_key
    JOIN dim_sector  s  ON t.sector_key   = s.sector_key
    WHERE d.year >= 2020
    GROUP BY ce.country_name, ci.country_name, s.sector_name, d.year
    ORDER BY export_bn DESC
    LIMIT 100
  "
  tryCatch(DBI::dbGetQuery(conn, sql), error = function(e) mock_trade_data())
}


# ════════════════════════════════════════════════════════
# MOCK DATA — used when DB is not available (demo mode)
# ════════════════════════════════════════════════════════

mock_kpi_data <- function() {
  list(
    gdp          = list(value = 27360.0,  change =  2.4),
    gdp_growth   = list(value =  2.5,     change =  0.3),
    inflation    = list(value =  3.1,     change = -0.8),
    unemployment = list(value =  3.9,     change = -0.2),
    trade_balance= list(value = -773.0,   change =  12.1),
    last_updated = format(Sys.time(), "%Y-%m-%d %H:%M UTC"),
    is_mock      = TRUE
  )
}

mock_gdp_trend <- function() {
  years    <- 2015:2024
  countries <- list(
    list(code="USA", name="United States", base=18e3),
    list(code="CHN", name="China",         base=11e3),
    list(code="IND", name="India",         base=2.1e3),
    list(code="DEU", name="Germany",       base=3.4e3),
    list(code="GBR", name="United Kingdom",base=2.9e3)
  )
  rows <- list()
  for (ctry in countries) {
    for (i in seq_along(years)) {
      growth <- rnorm(1, mean=2.5, sd=1.5)
      val    <- ctry$base * (1 + growth/100)^i
      rows[[length(rows)+1]] <- list(
        country_code = ctry$code,
        country_name = ctry$name,
        year         = years[i],
        gdp          = round(val/1e9, 1),
        growth       = round(growth, 2)
      )
    }
  }
  do.call(rbind, lapply(rows, as.data.frame))
}

mock_sector_data <- function() {
  sectors <- c("Information Technology","Financials","Health Care","Consumer Discretionary",
                "Industrials","Consumer Staples","Energy","Materials","Utilities",
                "Communication Services","Real Estate")
  years <- 2020:2024
  rows  <- list()
  for (s in sectors) {
    for (y in years) {
      for (q in 1:4) {
        rows[[length(rows)+1]] <- list(
          sector_name = s, year = y, quarter = q,
          return_pct  = round(rnorm(1, mean=2, sd=8), 2),
          pe_ratio    = round(runif(1, 12, 35), 1)
        )
      }
    }
  }
  do.call(rbind, lapply(rows, as.data.frame))
}

mock_country_map_data <- function() {
  data.frame(
    country_name = c("United States","China","India","Germany","United Kingdom",
                      "Japan","France","Brazil","Canada","Australia",
                      "South Korea","Mexico","Indonesia","Saudi Arabia","South Africa"),
    country_code = c("USA","CHN","IND","DEU","GBR","JPN","FRA","BRA","CAN","AUS","KOR","MEX","IDN","SAU","ZAF"),
    latitude     = c(37.09,35.86,20.59,51.17,55.38,36.20,46.23,-14.24,56.13,-25.27,35.91,23.63,-0.79,23.88,-30.56),
    longitude    = c(-95.71,104.19,78.96,10.45,-3.44,138.25,2.21,-51.93,-106.35,133.78,127.77,-102.55,113.92,45.08,22.94),
    region       = c("Americas","Asia","Asia","Europe","Europe","Asia","Europe","Americas","Americas","Oceania","Asia","Americas","Asia","Asia","Africa"),
    gdp          = c(27360,17960,3740,4430,3020,4210,2920,2130,2140,1690,1710,1320,1370,1060,390),
    gdp_growth   = c(2.5,5.2,6.8,0.1,0.4,2.0,1.1,3.0,1.4,2.0,1.4,2.5,5.0,4.2,0.6),
    inflation    = c(3.1,0.7,5.4,3.0,4.0,2.8,4.9,5.1,2.7,3.4,2.3,5.5,3.7,2.1,5.9),
    unemployment = c(3.9,5.2,7.9,3.0,4.2,2.6,7.3,7.9,5.8,3.7,2.7,2.9,5.3,4.4,32.1),
    stringsAsFactors = FALSE
  )
}

mock_trade_data <- function() {
  data.frame(
    exporter   = c("China","USA","Germany","Japan","South Korea"),
    importer   = c("USA","China","USA","USA","USA"),
    sector_name= c("Information Technology","Energy","Industrials","Consumer Discretionary","Information Technology"),
    year       = 2023,
    export_bn  = c(536.8, 153.2, 112.3, 142.6, 103.4),
    import_bn  = c(427.2, 148.5, 98.7, 125.3, 89.2),
    growth_pct = c(-4.2, 8.3, 2.1, -1.8, 5.6),
    stringsAsFactors = FALSE
  )
}
