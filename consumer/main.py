import boto3
from secret_keys import SecretKeys
import json

sqs_client = boto3.client("sqs", region_name= "eu-north-1")
ecs_client = boto3.client('ecs', region_name= "eu-north-1")

def poll_sqs():

    while True:
        response = sqs_client.receive_message(
            QueueUrl= SecretKeys().AWS_SQS_VIDEO_QUEUE_URL,
            MaxNumberOfMessages= 10,
            WaitTimeSeconds= 20,
        )
        print(response)

        for message in response.get("Messages", []):

            message_body = json.loads(message["Body"])

            if {
                "Service" in message_body 
                and "Event" in message_body
                and message_body.get("Event") == "s3:TestEvent"
            }:
                sqs_client.delete_message(
                    QueueUrl= SecretKeys().AWS_SQS_VIDEO_QUEUE_URL,
                    ReceiptHandle= message["ReceiptHandle"],
                )

            if "Records" in message_body:

                s3_records = message_body["Records"][0]["s3"]
                bucket_name = s3_records['bucket']['name']
                s3_key = s3_records["object"]["key"]

                response = ecs_client.run_task(
                    cluster = "arn:aws:ecs:eu-north-1:647582859160:cluster/My_transcoder_58",
                    launchType = "FARGATE",
                    taskDefinition = "arn:aws:ecs:eu-north-1:647582859160:task-definition/video_transcoder_58:1",
                    overrides = {
                        "containerOverrides":[
                            {
                            "name": "transcoder_58",
                            "environment": [
                                {"name": "S3_BUCKET", "value": bucket_name},
                                {"name": "S3_KEY", "value": s3_key},
                                ],
                            }
                        ] 
                        
                    },
                    networkConfiguration = {
                        "awsvpcConfiguration": {
                            "subnets": [
                                "subnet-093686b50422f742e",
                                "subnet-037ab7e676b44d289",
                                "subnet-0c2fab845d84cd519"
                            ],
                            "assignPublicIp": "ENABLED",
                            "securityGroups": [
                                "sg-07339bbca09061f8a"
                            ]
                        }
                    }
                )
                print(response)
                sqs_client.delete_message(
                    QueueUrl= SecretKeys().AWS_SQS_VIDEO_QUEUE_URL,
                    ReceiptHandle= message["ReceiptHandle"],
                )

poll_sqs() 