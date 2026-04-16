# ============================================================
# app.R — Economic Insights Dashboard
# R Shiny + Plumber API backend
# ============================================================

library(shiny)
library(DBI)
library(RPostgres)       # use RRedshift for production
library(dplyr)
library(jsonlite)
library(httr)
library(plotly)
library(lubridate)
library(tidyr)

# ── Load modules ─────────────────────────────────────────
source("R/data_fetch.R")
source("R/analytics.R")
source("R/charts.R")
source("R/utils.R")

# ── Configuration ────────────────────────────────────────
DB_CONFIG <- list(
  host     = Sys.getenv("DB_HOST",     "localhost"),
  port     = as.integer(Sys.getenv("DB_PORT", "5432")),
  dbname   = Sys.getenv("DB_NAME",    "economicdb"),
  user     = Sys.getenv("DB_USER",    "postgres"),
  password = Sys.getenv("DB_PASSWORD","postgres")
)

GMAPS_KEY  <- Sys.getenv("GOOGLE_MAPS_API_KEY", "")
FRED_KEY   <- Sys.getenv("FRED_API_KEY", "")
REFRESH_MS <- 30000   # 30s live refresh

# ── DB Connection pool ───────────────────────────────────
get_db_conn <- function() {
  tryCatch({
    DBI::dbConnect(
      RPostgres::Postgres(),
      host     = DB_CONFIG$host,
      port     = DB_CONFIG$port,
      dbname   = DB_CONFIG$dbname,
      user     = DB_CONFIG$user,
      password = DB_CONFIG$password
    )
  }, error = function(e) {
    message("DB connection failed: ", e$message, " — using mock data")
    NULL
  })
}


# ════════════════════════════════════════════════════════════
# UI — Serves the static HTML dashboard from www/index.html
# ════════════════════════════════════════════════════════════
ui <- fluidPage(
  tags$head(
    tags$script(HTML(paste0(
      "var GMAPS_KEY = '", GMAPS_KEY, "';"
    )))
  ),
  uiOutput("dashboard")
)


# ════════════════════════════════════════════════════════════
# SERVER
# ════════════════════════════════════════════════════════════
server <- function(input, output, session) {

  # ── Serve static HTML dashboard ──────────────────────
  output$dashboard <- renderUI({
    includeHTML("www/index.html")
  })

  # ── Reactive DB connection ────────────────────────────
  conn <- reactiveVal(NULL)
  observe({
    con <- get_db_conn()
    conn(con)
    if (is.null(con)) {
      message("Running on mock data")
    }
  })

  # ── Auto-refresh timer ────────────────────────────────
  timer <- reactiveTimer(REFRESH_MS)

  # ── KPI Data ─────────────────────────────────────────
  kpi_data <- reactive({
    timer()
    tryCatch({
      if (!is.null(conn())) {
        fetch_kpi_summary(conn())
      } else {
        mock_kpi_data()
      }
    }, error = function(e) mock_kpi_data())
  })

  # ── GDP Trend ─────────────────────────────────────────
  gdp_trend <- reactive({
    timer()
    tryCatch({
      if (!is.null(conn())) {
        fetch_gdp_trend(conn(), year_start = 2015)
      } else {
        mock_gdp_trend()
      }
    }, error = function(e) mock_gdp_trend())
  })

  # ── Sector Heatmap ────────────────────────────────────
  sector_data <- reactive({
    timer()
    tryCatch({
      if (!is.null(conn())) {
        fetch_sector_heatmap(conn())
      } else {
        mock_sector_data()
      }
    }, error = function(e) mock_sector_data())
  })

  # ── Country Map Data ──────────────────────────────────
  country_map <- reactive({
    timer()
    tryCatch({
      if (!is.null(conn())) {
        fetch_country_map_data(conn())
      } else {
        mock_country_map_data()
      }
    }, error = function(e) mock_country_map_data())
  })

  # ── Trade Flow ────────────────────────────────────────
  trade_data <- reactive({
    timer()
    tryCatch({
      if (!is.null(conn())) {
        fetch_trade_flows(conn())
      } else {
        mock_trade_data()
      }
    }, error = function(e) mock_trade_data())
  })


  # ════════════════════════════════════════════════════════
  # API ENDPOINTS (consumed by JavaScript dashboard)
  # ════════════════════════════════════════════════════════

  # /api/kpis
  output$api_kpis <- renderText({
    data <- kpi_data()
    toJSON(data, auto_unbox = TRUE, na = "null")
  })

  # /api/gdp_trend
  output$api_gdp_trend <- renderText({
    toJSON(gdp_trend(), auto_unbox = TRUE, na = "null")
  })

  # /api/sectors
  output$api_sectors <- renderText({
    toJSON(sector_data(), auto_unbox = TRUE, na = "null")
  })

  # /api/map
  output$api_map <- renderText({
    toJSON(country_map(), auto_unbox = TRUE, na = "null")
  })

  # /api/trade
  output$api_trade <- renderText({
    toJSON(trade_data(), auto_unbox = TRUE, na = "null")
  })

  # ── Expose data to JS via Shiny.setInputValue ─────────
  observe({
    timer()
    session$sendCustomMessage("update_kpis",    kpi_data())
    session$sendCustomMessage("update_gdp",     gdp_trend())
    session$sendCustomMessage("update_sectors", sector_data())
    session$sendCustomMessage("update_map",     country_map())
    session$sendCustomMessage("update_trade",   trade_data())
    session$sendCustomMessage("update_ts",      list(ts = format(Sys.time(), "%H:%M:%S")))
  })

  # ── Session cleanup ──────────────────────────────────
  session$onSessionEnded(function() {
    con <- conn()
    if (!is.null(con)) DBI::dbDisconnect(con)
    message("Session ended, DB disconnected")
  })
}

# ── Run App ──────────────────────────────────────────────
shinyApp(ui = ui, server = server)
