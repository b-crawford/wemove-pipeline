import os
import boto3


def lambda_handler(event, context):
    # name the job
    job_name = "weca-process-4"

    # set up client
    client = boto3.client("glue")

    arguments = {
        "--s3-upload-bucket": event["Records"][0]["s3"]["bucket"]["name"],
        "--s3-upload-object-key": event["Records"][0]["s3"]["object"]["key"],
        "--s3-output-bucket": os.environ["OUTPUT_BUCKET"],
    }
    response = client.start_job_run(JobName=job_name, Arguments=arguments)

    return {"statusCode": 200, "body": response["JobRunId"]}
