#!/usr/bin/env python3
import os

import aws_cdk as cdk

from health_data_lake.health_data_lake_stack import HealthDataLakeStack


app = cdk.App()

HealthDataLakeStack(app, 
    "health-datalake-dev",
    env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'),
    region=os.getenv('CDK_DEFAULT_REGION')),
)

HealthDataLakeStack(app, 
    "health-datalake-prod",
    env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'),
    region=os.getenv('CDK_DEFAULT_REGION')),
)

app.synth()
