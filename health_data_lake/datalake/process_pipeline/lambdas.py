from constructs import Construct
from aws_cdk import (
    aws_lambda as _lambda,
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_glue as _glue,
    Duration
)

class DatalakeLambdas(Construct):

    @property
    def functions_list(self):
        return self.cleaner_lambda, self.invoke_crawler_lambda, self.check_crawler_lambda

    def __init__(self, 
        scope: Construct, 
        id: str, 
        raw_bucket: _s3.Bucket, 
        cleaned_bucket: _s3.Bucket, 
        curated_bucket: _s3.Bucket,
        support_bucket: _s3.Bucket,
        crawler_name: str,
        **kwargs
    ):

        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()

# ========================================================================
# ====================== CLEANER LAMBDA FUNCTION =========================
# ========================================================================
        cleaner_lambda_role = _iam.Role(
            self, 'cleaner-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        cleaner_lambda_policy = _iam.Policy(
            self, 'cleaner-lambda-role-policy',
            roles=[cleaner_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                        "s3:PutObject"
                    ],
                    resources= [
                        raw_bucket.bucket_arn,
                        f'{raw_bucket.bucket_arn}/*',
                        cleaned_bucket.bucket_arn,
                        f'{cleaned_bucket.bucket_arn}/*'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

        wrangler_lambda_layer = _lambda.LayerVersion(self, 'wrangler-layer',
                  code = _lambda.AssetCode('src/lambda/layers/awswrangler-layer-2.17.0-py3.9.zip'),
                  compatible_runtimes = [_lambda.Runtime.PYTHON_3_9],
        )      

        self.cleaner_lambda = _lambda.Function(
            self, 'cleaner-obs-sample',
            function_name=f'{self.stack_name}-cleaner-obs-sample',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=1024,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/cleaner/'),
            handler='cleaner.handler',
            role=cleaner_lambda_role,
            layers = [wrangler_lambda_layer],
            environment={
                "DESTINATION_BUCKET_NAME": cleaned_bucket.bucket_name
            }
        )

# ========================================================================
# =============== INVOKE CRAWLER SERVERLESS LAMBDA =======================
# ======================================================================== 

# ======================== INVOKE LAMBDA ROLE ============================
        invoke_crawler_lambda_role = _iam.Role(
            self, 'invoke-crawler-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        invoke_crawler_lambda_policy = _iam.Policy(
            self, 'invoke-crawler-lambda-role-policy',
            roles=[invoke_crawler_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "glue:StartCrawler",
                        "glue:GetCrawler"
                    ],
                    resources= [
                        f'arn:aws:glue:{scope.region}:{scope.account}:crawler/{crawler_name}'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

# ===================== INVOKE LAMBDA FUNCTION ===========================

        self.invoke_crawler_lambda = _lambda.Function(
            self, 'invoke-crawler',
            function_name=f'{self.stack_name}-invoke-crawler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/invoke_crawler/'),
            handler='invoke_crawler.handler',
            role=invoke_crawler_lambda_role,
            environment={
                "CRAWLER_NAME": crawler_name
            }
        )

# ========================================================================
# =============== CHECK CRAWLER SERVERLESS LAMBDA =======================
# ======================================================================== 

# ======================== CHECK LAMBDA ROLE ============================
        check_crawler_lambda_role = _iam.Role(
            self, 'check-crawler-lambda-role',
            assumed_by= _iam.ServicePrincipal("lambda.amazonaws.com")
        )

        check_crawler_lambda_policy = _iam.Policy(
            self, 'check-crawler-lambda-role-policy',
            roles=[check_crawler_lambda_role],
            statements=[_iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "glue:GetCrawler"
                    ],
                    resources= [
                        f'arn:aws:glue:{scope.region}:{scope.account}:crawler/{crawler_name}'
                    ]
            ),
            _iam.PolicyStatement(
                    effect= _iam.Effect.ALLOW,
                    actions= [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources= [
                        '*'
                    ]
            )]
        )

# ===================== CHECK LAMBDA FUNCTION ===========================

        self.check_crawler_lambda = _lambda.Function(
            self, 'check-crawler',
            function_name=f'{self.stack_name}-check-crawler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            memory_size=256,
            timeout=Duration.minutes(5),
            code=_lambda.Code.from_asset('src/lambda/check_crawler/'),
            handler='check_crawler.handler',
            role=check_crawler_lambda_role,
            environment={
                "CRAWLER_NAME": crawler_name
            }
        )


# ========================================================================