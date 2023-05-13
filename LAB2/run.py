try:
    import json
    import uuid
    import os
    import boto3
    from dotenv import load_dotenv

    load_dotenv("../.env")
except Exception as e:
    pass

global AWS_ACCESS_KEY
global AWS_SECRET_KEY
global AWS_REGION_NAME

AWS_ACCESS_KEY = os.getenv("DEV_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("DEV_SECRET_KEY")
AWS_REGION_NAME = os.getenv("DEV_REGION")

client = boto3.client("emr-serverless",
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGION_NAME)


def lambda_handler_test_emr(event, context):
    # ------------------Hudi settings ---------------------------------------------
    bucket = "xxx"
    # ---------------------------------------------------------------------------------
    #                                       EMR
    # --------------------------------------------------------------------------------
    ApplicationId = "xxx"
    ExecutionTime = 600
    ExecutionArn = os.getenv("ExecutionArn")
    JobName = 'hive_jobs_{}'.format(uuid.uuid4().__str__())

    # --------------------------------------------------------------------------------
    hive_submit_parameters = ""
    hive_submit_parameters += f' --hiveconf hive.exec.scratchdir=s3://{bucket}/hive/scratch'
    hive_submit_parameters  += f" --hiveconf hive.metastore.warehouse.dir=s3://{bucket}/hive/warehouse"

    response = client.start_job_run(
        applicationId=ApplicationId,
        clientToken=uuid.uuid4().__str__(),
        executionRoleArn=ExecutionArn,

        jobDriver={
            "hive": {
                "initQueryFile": f"s3://{bucket}/sql/initQueryFile.sql",
                "query": f"s3://{bucket}/sql/query.sql",
                "parameters": hive_submit_parameters
            },
        },
        configurationOverrides={
            'applicationConfiguration': [
                {
                    "classification": "hive-site",
                    "properties": {
                        "hive.driver.cores": "2",
                        "hive.driver.memory": "4g",
                        "hive.tez.container.size": "8192",
                        "hive.tez.cpu.vcores": "4"
                    }
                }
            ]
        },
        executionTimeoutMinutes=ExecutionTime,
        name=JobName,
    )
    print("response", end="\n")
    print(response)


lambda_handler_test_emr(context=None, event=None)
