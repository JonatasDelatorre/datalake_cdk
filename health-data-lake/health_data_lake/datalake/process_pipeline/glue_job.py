from constructs import Construct
from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_glue as _glue,
    aws_s3_deployment as _s3_deployment,
    Duration
)

class DatalakeGlueJobs(Construct):

    @property
    def job_list(self):
        return self.process_glue_job

    def __init__(self, 
        scope: Construct, 
        id: str, 
        cleaned_bucket: _s3.Bucket, 
        curated_bucket: _s3.Bucket,
        support_bucket: _s3.Bucket,
        **kwargs
    ):

        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()
# ========================================================================
# ====================== CLEANER LAMBDA FUNCTION =========================
# ========================================================================

        _s3_deployment.BucketDeployment(
            self, "deploy_glue_script_s3",
            sources=[_s3_deployment.Source.asset("./src/glue_job")],  
            destination_bucket=support_bucket,
            destination_key_prefix="glue_job/" 
        )

        # Caminho do script no S3
        script_path = support_bucket.s3_url_for_object("glue_job/process.py")

        # Role para o Glue Job
        glue_role = _iam.Role(
            self, "process-glue-job-role",
            assumed_by=_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        support_bucket.grant_read(glue_role)
        cleaned_bucket.grant_read(glue_role)
        curated_bucket.grant_read(glue_role)
        curated_bucket.grant_write(glue_role) 

        self.process_glue_job = _glue.CfnJob(
            self, "process-glue-job",
            name=f"{self.stack_name}-process-job",
            role=glue_role.role_arn,
            command=_glue.CfnJob.JobCommandProperty(
                name="glueetl",  # Tipo de job (PySpark: glueetl, Python Shell: pythonshell)
                script_location=script_path  # Caminho do script no S3
            ),
            default_arguments={
                "--TempDir": f"s3://{support_bucket.bucket_name}/tmp/",
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true"
            },
            execution_property=_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version="3.0",
            max_capacity=2 
        )
