"""
AWS Lambda: Pipeline Trigger
Triggered by: S3 PUT event OR EventBridge schedule (daily 02:00 UTC)
"""

import json
import logging
import os
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME        = os.environ.get('GLUE_JOB_NAME', 'economic-etl')
RAW_BUCKET           = os.environ.get('RAW_BUCKET',     'economic-insights-raw')
PROCESSED_BUCKET     = os.environ.get('PROCESSED_BUCKET', 'economic-insights-processed')
REDSHIFT_CONN        = os.environ.get('REDSHIFT_CONN',  'economic-redshift')
REDSHIFT_DB          = os.environ.get('REDSHIFT_DB',    'economicdb')
SNS_TOPIC_ARN        = os.environ.get('SNS_TOPIC_ARN',  '')
REDSHIFT_TABLE_PREFIX= os.environ.get('TABLE_PREFIX',   '')

glue = boto3.client('glue')
sns  = boto3.client('sns')


def lambda_handler(event, context):
    """
    Entry point. Handles:
    - S3 trigger (ObjectCreated)
    - EventBridge schedule
    - Manual invocation
    """
    pipeline_run_id = datetime.utcnow().strftime('run_%Y%m%d_%H%M%S')
    trigger_source  = _detect_source(event)

    logger.info(f"Trigger: {trigger_source} | Run ID: {pipeline_run_id}")

    try:
        # ── Validate no concurrent job running ──
        _check_no_concurrent_run()

        # ── Start Glue ETL job ──
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--S3_RAW_BUCKET':         RAW_BUCKET,
                '--S3_PROCESSED_BUCKET':   PROCESSED_BUCKET,
                '--REDSHIFT_CONN':         REDSHIFT_CONN,
                '--REDSHIFT_DB':           REDSHIFT_DB,
                '--REDSHIFT_TABLE_PREFIX': REDSHIFT_TABLE_PREFIX,
                '--PIPELINE_RUN_ID':       pipeline_run_id,
                '--enable-metrics':        '',
            }
        )

        job_run_id = response['JobRunId']
        logger.info(f"Glue job started: {job_run_id}")

        # ── Notify via SNS ──
        _notify(
            subject="✅ Economic Pipeline Started",
            message=f"Job: {GLUE_JOB_NAME}\nRun ID: {job_run_id}\nPipeline: {pipeline_run_id}"
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status':         'started',
                'glue_run_id':    job_run_id,
                'pipeline_run_id': pipeline_run_id
            })
        }

    except glue.exceptions.ConcurrentRunsExceededException:
        logger.warning("Glue job already running — skipping")
        return {'statusCode': 409, 'body': json.dumps({'status': 'skipped', 'reason': 'concurrent_run'})}

    except Exception as e:
        logger.error(f"Lambda error: {e}")
        _notify(
            subject="❌ Economic Pipeline Failed to Start",
            message=f"Error: {str(e)}"
        )
        raise


def _detect_source(event: dict) -> str:
    if 'Records' in event:
        record = event['Records'][0]
        if record.get('eventSource') == 'aws:s3':
            key = record['s3']['object']['key']
            return f"S3 trigger: {key}"
    if 'source' in event and event['source'] == 'aws.events':
        return "EventBridge schedule"
    return "Manual invocation"


def _check_no_concurrent_run():
    runs = glue.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=5)
    for run in runs.get('JobRuns', []):
        if run['JobRunState'] in ('STARTING', 'RUNNING', 'STOPPING'):
            raise glue.exceptions.ConcurrentRunsExceededException(
                error_response={'Error': {'Code': 'ConcurrentRunsExceededException'}},
                operation_name='StartJobRun'
            )


def _notify(subject: str, message: str):
    if not SNS_TOPIC_ARN:
        return
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
    except Exception as e:
        logger.warning(f"SNS notify failed: {e}")
