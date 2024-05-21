# annotator.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2013-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import subprocess
import os
import sys
import time
from subprocess import Popen, PIPE
from botocore.exceptions import ClientError

from annotator_webhook import app
# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")
 
"""Reads request messages from SQS and runs AnnTools as a subprocess.

Move existing annotator code here
"""


def handle_requests_queue(sqs=None):
    try:
        sqs_queue_url = config.get("sns", "Sqs_queue_url")
        
        response = sqs.receive_message(
        QueueUrl=sqs_queue_url,
        AttributeNames=[
            'All'
        ],
        MaxNumberOfMessages=int(config.get("sqs", "MaxMessages")),
        MessageAttributeNames=[
            'All'
        ],
        WaitTimeSeconds=int(config.get("sqs", "WaitTime"))
        )
        messages = response.get('Messages', [])
        for message in messages:
            # Extract job parameters from message body
            job_data = json.loads(json.loads(message['Body'])["Message"])
            print(job_data)
            # print(type(job_data))

            job_id = job_data['job_id']
            input_file_name = job_data['input_file_name']
            s3_inputs_bucket = job_data['s3_inputs_bucket']
            s3_key_input_file = job_data['s3_key_input_file']
            user_id = job_data['user_id']
            print(s3_key_input_file)
            # Get the input file S3 object and copy it to a local file
            local_file_path = "/home/ubuntu/gas/ann/data/"+user_id + ":" + job_id + "~" + input_file_name
            print(local_file_path)
            s3 = boto3.client('s3')
            s3.download_file(s3_inputs_bucket, s3_key_input_file, local_file_path)
            # update in kvs
            table_name = config.get("gas","AnnotationsTable")
            new_value = 'RUNNING'
            dynamodb = boto3.resource('dynamodb', region_name=config.get("aws", "AwsRegionName"))
            table = dynamodb.Table(table_name)
            response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET #job_status = :job_status',
            ExpressionAttributeNames={'#job_status': 'job_status'},
            ConditionExpression='#job_status = :expected_job_status',
            ExpressionAttributeValues={':job_status': new_value,
                                        ':expected_job_status': "PENDING"},
            ReturnValues='UPDATED_NEW'
            )
            # print(response)

            # Launch annotation job as a background process
            command = ["python", "/home/ubuntu/gas/ann/run.py", local_file_path]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Delete message from the queue, if the job was successfully submitted
            res = sqs.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
    except Exception as e:    
        # Handle error, log, or retry logic as needed 
        print(f"Error processing message: {str(e)}")
        


def main():

    # Get handles to queue
    sqs_client = boto3.client('sqs', region_name=config.get("aws", "AwsRegionName"))
    # Poll queue for new results and process them
    while True:
        handle_requests_queue(sqs_client)


if __name__ == "__main__":
    main()

### EOF

