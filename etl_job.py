"""
AWS Glue ETL Job: Economic Data Pipeline
Raw S3 → Processed S3 → Redshift

Run: aws glue start-job-run --job-name economic-etl
"""

import sys
import json
import logging
from datetime import datetime, date
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3
import requests

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Glue Context ─────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_RAW_BUCKET',
    'S3_PROCESSED_BUCKET',
    'REDSHIFT_CONN',
    'REDSHIFT_DB',
    'REDSHIFT_TABLE_PREFIX',
    'PIPELINE_RUN_ID'
])

sc        = SparkContext()
glueCtx   = GlueContext(sc)
spark     = glueCtx.spark_session
job       = Job(glueCtx)
job.init(args['JOB_NAME'], args)

RAW_BUCKET       = args['S3_RAW_BUCKET']
PROCESSED_BUCKET = args['S3_PROCESSED_BUCKET']
PIPELINE_RUN_ID  = args.get('PIPELINE_RUN_ID', datetime.utcnow().strftime('%Y%m%d_%H%M%S'))

logger.info(f"Pipeline run: {PIPELINE_RUN_ID}")


# ═══════════════════════════════════════════════════════════
# STEP 1: INGEST — World Bank API → S3 Raw
# ═══════════════════════════════════════════════════════════

COUNTRIES  = ['US','CN','IN','DE','GB','JP','FR','BR','CA','AU','KR','MX','ID','SA','ZA']
INDICATORS = [
    'NY.GDP.MKTP.CD',
    'NY.GDP.MKTP.KD.ZG',
    'FP.CPI.TOTL.ZG',
    'SL.UEM.TOTL.ZS',
    'BN.CAB.XOKA.CD',
    'NE.EXP.GNFS.ZS',
    'NE.IMP.GNFS.ZS',
    'GC.DOD.TOTL.GD.ZS',
    'FR.INR.RINR',
    'SP.POP.TOTL',
]


def fetch_world_bank(indicator: str, countries: list, year_start=2010, year_end=2024) -> list:
    """Fetch data from World Bank API v2."""
    country_str = ";".join(countries)
    url = (
        f"https://api.worldbank.org/v2/country/{country_str}/indicator/{indicator}"
        f"?format=json&per_page=2000&mrv=15&date={year_start}:{year_end}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if len(data) < 2 or not data[1]:
            logger.warning(f"No data for {indicator}")
            return []
        records = []
        for row in data[1]:
            if row.get('value') is None:
                continue
            records.append({
                'indicator_code':  indicator,
                'country_code_2':  row['countryiso3code'][:3] if row.get('countryiso3code') else row['country']['id'],
                'period':          str(row['date']),
                'value':           float(row['value']),
                'unit':            row.get('unit', ''),
                'source':          'World Bank',
                'pipeline_run_id': PIPELINE_RUN_ID,
                'loaded_at':       datetime.utcnow().isoformat()
            })
        logger.info(f"Fetched {len(records)} records for {indicator}")
        return records
    except Exception as e:
        logger.error(f"World Bank fetch error {indicator}: {e}")
        return []


def ingest_to_s3():
    """Fetch all indicators and save as Parquet to S3 raw zone."""
    all_records = []
    for ind in INDICATORS:
        records = fetch_world_bank(ind, COUNTRIES)
        all_records.extend(records)

    if not all_records:
        logger.error("No records fetched — aborting")
        return False

    # Convert to Spark DF
    schema = StructType([
        StructField("indicator_code",  StringType(), True),
        StructField("country_code_2",  StringType(), True),
        StructField("period",          StringType(), True),
        StructField("value",           DoubleType(),  True),
        StructField("unit",            StringType(), True),
        StructField("source",          StringType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("loaded_at",       StringType(), True),
    ])

    raw_df = spark.createDataFrame(all_records, schema)
    s3_path = f"s3://{RAW_BUCKET}/world_bank/run={PIPELINE_RUN_ID}/"
    raw_df.write.mode("overwrite").parquet(s3_path)
    logger.info(f"Saved {raw_df.count()} raw records to {s3_path}")
    return True


# ═══════════════════════════════════════════════════════════
# STEP 2: TRANSFORM — Spark Processing
# ═══════════════════════════════════════════════════════════

COUNTRY_ISO2_TO_ISO3 = {
    'US':'USA','CN':'CHN','IN':'IND','DE':'DEU','GB':'GBR','JP':'JPN',
    'FR':'FRA','BR':'BRA','CA':'CAN','AU':'AUS','KR':'KOR','MX':'MEX',
    'ID':'IDN','SA':'SAU','ZA':'ZAF','RU':'RUS','TR':'TUR','AR':'ARG',
    'SG':'SGP','NL':'NLD'
}
country_map_expr = F.create_map([F.lit(k) for pair in COUNTRY_ISO2_TO_ISO3.items() for k in pair])


def transform_raw():
    """Apply business rules, YoY calculations, data quality checks."""
    s3_path = f"s3://{RAW_BUCKET}/world_bank/"
    raw_df  = spark.read.parquet(s3_path)

    # ── Country code standardisation ──
    df = raw_df.withColumn(
        'country_iso3',
        F.when(
            F.length('country_code_2') == 3, F.col('country_code_2')
        ).otherwise(
            country_map_expr.getItem(F.col('country_code_2'))
        )
    ).filter(F.col('country_iso3').isNotNull())

    # ── Parse year/quarter from period ──
    df = df.withColumn('year',
        F.when(F.col('period').rlike(r'^\d{4}$'), F.col('period').cast(IntegerType()))
         .when(F.col('period').rlike(r'^\d{4}Q\d$'), F.substring('period', 1, 4).cast(IntegerType()))
         .otherwise(None)
    ).withColumn('quarter',
        F.when(F.col('period').rlike(r'^\d{4}Q(\d)$'),
               F.regexp_extract('period', r'Q(\d)', 1).cast(IntegerType()))
         .otherwise(None)
    ).withColumn('date_key',
        F.when(F.col('quarter').isNull(),
               F.concat(F.col('year'), F.lit('1231')).cast(IntegerType()))  # year-end
         .otherwise(
               F.concat(F.col('year'),
                   F.when(F.col('quarter')==1,F.lit('0331'))
                    .when(F.col('quarter')==2,F.lit('0630'))
                    .when(F.col('quarter')==3,F.lit('0930'))
                    .otherwise(F.lit('1231'))
               ).cast(IntegerType())
         )
    )

    # ── YoY calculation via window ──
    w = Window.partitionBy('country_iso3','indicator_code').orderBy('year','quarter')
    df = df.withColumn('prev_value', F.lag('value', 1).over(w)) \
           .withColumn('yoy_change',
               F.round((F.col('value') - F.col('prev_value')) / F.abs(F.col('prev_value')) * 100, 4)
           )

    # ── Data quality: flag anomalies ──
    df = df.withColumn('is_anomaly',
        (F.abs(F.col('yoy_change')) > 200)  # >200% YoY change flagged
    )

    # ── Select final columns ──
    final = df.select(
        'date_key',
        F.col('country_iso3').alias('country_code'),
        'indicator_code',
        'source',
        'value',
        'yoy_change',
        F.lit(False).alias('is_estimated'),
        'is_anomaly',
        'pipeline_run_id',
        F.current_timestamp().alias('load_timestamp')
    ).filter(F.col('is_anomaly') == False) \
     .filter(F.col('value').isNotNull())

    out_path = f"s3://{PROCESSED_BUCKET}/fact_indicators/run={PIPELINE_RUN_ID}/"
    final.write.mode("overwrite").parquet(out_path)
    logger.info(f"Transformed {final.count()} records → {out_path}")
    return out_path


# ═══════════════════════════════════════════════════════════
# STEP 3: LOAD — Processed S3 → Redshift
# ═══════════════════════════════════════════════════════════

def load_to_redshift(processed_path: str):
    """Load processed Parquet into Redshift via COPY command."""
    conn_opts = glueCtx.extract_jdbc_conf(args['REDSHIFT_CONN'])

    processed_df = spark.read.parquet(processed_path)

    # Write via Glue DynamicFrame JDBC
    from awsglue.dynamicframe import DynamicFrame
    dyf = DynamicFrame.fromDF(processed_df, glueCtx, "processed")

    glueCtx.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection=args['REDSHIFT_CONN'],
        connection_options={
            "dbtable":  f"{args['REDSHIFT_TABLE_PREFIX']}stg_raw_indicators",
            "database": args['REDSHIFT_DB'],
            "preactions": "TRUNCATE TABLE stg_raw_indicators;",
            "postactions": """
                INSERT INTO fact_economic_indicators (date_key,country_key,indicator_key,source_key,value,yoy_change,pipeline_run_id,load_timestamp)
                SELECT
                    s.date_key,
                    c.country_key,
                    i.indicator_key,
                    src.source_key,
                    s.value,
                    s.yoy_change,
                    s.pipeline_run_id,
                    s.load_timestamp
                FROM stg_raw_indicators s
                JOIN dim_country   c   ON c.country_code   = s.country_code
                JOIN dim_indicator i   ON i.indicator_code = s.indicator_code
                JOIN dim_source    src ON src.source_name  = s.source
                WHERE NOT EXISTS (
                    SELECT 1 FROM fact_economic_indicators f2
                    WHERE f2.date_key      = s.date_key
                      AND f2.country_key  = c.country_key
                      AND f2.indicator_key= i.indicator_key
                );
            """
        },
        redshift_tmp_dir=f"s3://{PROCESSED_BUCKET}/tmp/"
    )
    logger.info("Redshift load complete")


# ═══════════════════════════════════════════════════════════
# STEP 4: PUBLISH METRICS
# ═══════════════════════════════════════════════════════════

def publish_metrics(record_count: int, status: str):
    """Publish pipeline metrics to CloudWatch."""
    cw = boto3.client('cloudwatch', region_name='us-east-1')
    cw.put_metric_data(
        Namespace='EconomicInsights/Pipeline',
        MetricData=[
            {'MetricName': 'RecordsLoaded',    'Value': record_count, 'Unit': 'Count'},
            {'MetricName': 'PipelineSuccess',  'Value': 1 if status=='success' else 0, 'Unit': 'Count'},
        ]
    )
    logger.info(f"Published CloudWatch metrics: {record_count} records, status={status}")


# ═══════════════════════════════════════════════════════════
# ORCHESTRATE
# ═══════════════════════════════════════════════════════════

try:
    logger.info("=== PHASE 1: INGEST ===")
    ingest_ok = ingest_to_s3()

    if ingest_ok:
        logger.info("=== PHASE 2: TRANSFORM ===")
        processed_path = transform_raw()

        logger.info("=== PHASE 3: LOAD ===")
        load_to_redshift(processed_path)

        publish_metrics(record_count=1000, status='success')
        logger.info("✅ Pipeline completed successfully")
    else:
        publish_metrics(0, 'failed')
        raise RuntimeError("Ingestion failed")

except Exception as e:
    logger.error(f"Pipeline failed: {e}")
    publish_metrics(0, 'failed')
    raise

finally:
    job.commit()
