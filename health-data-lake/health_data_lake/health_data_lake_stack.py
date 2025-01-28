from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as _s3,
)

from .datalake.data.buckets import DatalakeBuckets

from .datalake.data.glue_crawlers import DataCatalogs
from .datalake.process_pipeline.lambdas import DatalakeLambdas
from .datalake.process_pipeline.glue_job import DatalakeGlueJobs

from .datalake.orchestration.stepfunctions import DatalakeProcessSTF
from .datalake.orchestration.event_bridge import StfEventBridgeS3

class HealthDataLakeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ========================================================================
        # ======================== DATALAKE BUCKETS ==============================
        # ========================================================================
        datalake_buckets = DatalakeBuckets(
            self, 'DatalakeBuckets',
        )
        raw_bucket, cleaned_bucket, curated_bucket, support_bucket = datalake_buckets.bucket_list

        # ========================================================================
        # ========================== DATA CATALOG ================================
        # ========================================================================

        data_catalogs = DataCatalogs(
            self, 'DataCatalogs',
            curated_bucket=curated_bucket
        )

        crawler_name = data_catalogs.get_crawler_name

        # ========================================================================
        # ======================== PROCESS PIPELINE ==============================
        # ========================================================================

        lambda_functions = DatalakeLambdas(
            self, 'DatalakeLambdas',
            raw_bucket=raw_bucket,
            cleaned_bucket=cleaned_bucket,
            curated_bucket=curated_bucket,
            support_bucket=support_bucket,
            crawler_name=crawler_name
        )

        cleaner_lambda, invoke_crawler_lambda, check_crawler_lambda = lambda_functions.functions_list


        glue_jobs = DatalakeGlueJobs(
            self, 'DatalakeGlueJobs',
            cleaned_bucket=cleaned_bucket,
            curated_bucket=curated_bucket,
            support_bucket=support_bucket
        )

        process_glue_job = glue_jobs.job_list

        # ========================================================================
        # ========================= ORCHESTRATION ================================
        # ========================================================================

        # Stepfunctions definition
        step_functions = DatalakeProcessSTF(
            self, 'DatalakeProcessSTF',
            cleaner_lambda=cleaner_lambda,
            process_glue_job=process_glue_job,
            invoke_crawler_lambda=invoke_crawler_lambda,
            check_crawler_lambda=check_crawler_lambda
        )

        state_machine = step_functions.get_stepfunctions

        # Event Bridge definition
        stf_event_from_s3 = StfEventBridgeS3(
            self, 'StfEventBridgeS3',
            raw_bucket=raw_bucket,
            state_machine=state_machine
        )