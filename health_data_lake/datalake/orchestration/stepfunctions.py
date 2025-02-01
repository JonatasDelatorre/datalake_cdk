from constructs import Construct
from aws_cdk import (
    aws_stepfunctions as _stf,
    aws_stepfunctions_tasks as _stf_tasks,
    aws_lambda as _lambda,
    aws_glue as _glue,
    aws_iam as _iam,
    Duration
)


class DatalakeProcessSTF(Construct):

    @property
    def get_stepfunctions(self):
        return self.stf

    def __init__(self,
                 scope: Construct,
                 id: str,
                 cleaner_lambda: _lambda.Function,
                 process_glue_job: _glue.CfnJob,
                 invoke_crawler_lambda: _lambda.Function,
                 check_crawler_lambda: _lambda.Function,
                 **kwargs
                 ):
        super().__init__(scope, id, **kwargs)

        self.stack_name = scope.to_string()

        clear_task = _stf_tasks.LambdaInvoke(
            self, "Clear Data Task",
            lambda_function=cleaner_lambda,
            output_path="$.Payload",
        )

        process_task = _stf_tasks.GlueStartJobRun(
            self, "Process Data Task",
            glue_job_name=process_glue_job.name,
            integration_pattern=_stf.IntegrationPattern.RUN_JOB
        )

        invoke_crawler_task = _stf_tasks.LambdaInvoke(
            self, "Invoke Crawler Catalog Task",
            lambda_function=invoke_crawler_lambda,
            output_path="$.Payload",
        )

        check_crawler_task = _stf_tasks.LambdaInvoke(
            self, "Check Crawler Catalog Task",
            lambda_function=check_crawler_lambda,
            result_path="$.check_crawler_result"
        )

        succeed_job = _stf.Succeed(
            self, "Succeeded",
            comment='AWS Batch Job succeeded'
        )

        wait_crawler_job = _stf.Wait(
            self, "Wait Crawler 20 Seconds",
            time=_stf.WaitTime.duration(
                Duration.seconds(20))
        )

        definition = clear_task \
            .next(process_task) \
            .next(invoke_crawler_task) \
            .next(check_crawler_task) \
            .next(_stf.Choice(self, 'Crawler Complete?')
                .when(_stf.Condition.string_equals('$.check_crawler_result.Payload', 'SUCCEEDED'),
                    succeed_job)
                .otherwise(wait_crawler_job
                            .next(check_crawler_task)
                        )
            )

        stepfunctions_role = _iam.Role(
            self, "StepFunctionsExecutionRole",
            assumed_by=_iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),  # Permissões para o Glue
                _iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaRole"),  # Permissões para Lambda
            ]
        )

        # Create state machine
        self.stf = _stf.StateMachine(
            self, "StateMachine Pipeline",
            state_machine_name=f"{self.stack_name}-state-machine",
            definition_body=_stf.DefinitionBody.from_chainable(definition),
            role=stepfunctions_role,
            timeout=Duration.minutes(15)
        )
