# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError
from datetime import datetime

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("archive_script_config.ini")

"""A14
Archive free user results files
"""
#reference
# https://stackoverflow.com/questions/41833565/s3-buckets-to-glacier-on-demand-is-it-possible-from-boto3-api#:~:text=The%20only%20way%20to%20migrate,class%20is%20through%20lifecycle%20policies.&text=Constraints%3A%20You%20cannot%20specify%20GLACIER,you%20can%20use%20lifecycle%20configuration.
# https://docs.aws.amazon.com/code-library/latest/ug/python_3_glacier_code_examples.html
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html

def move_files_to_glacier(bucket_name, vault_name, file_key):
    full_file_key = f"{bucket_name}/{file_key}"
    glacier = boto3.client('glacier', region_name=config.get("aws", "AwsRegionName"))
    response = glacier.upload_archive(vaultName=vault_name, archiveDescription='Archive from gas-results bucket', body=full_file_key)
    print(response)
    return response['archiveId']


def handle_archive_queue(sqs=None):

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
        print(response)
        messages = response.get('Messages', [])
        for message in messages:
            job_data = json.loads(json.loads(message['Body'])["Message"])
            job_id = job_data['job_id']
            user_id = job_data['user_id']
            res_file = job_data['s3_key_result_file']
            complete_time = int(job_data['complete_time'])
            if (time.time() - complete_time > int(config.get('gas','time'))):
                profile = helpers.get_user_profile(id=user_id, db_name=config.get('aws', "AwsAccount"))
                print(profile[4])
                if profile[4] == "free_user":
                                
                    gid = move_files_to_glacier(config.get('s3', "ResultBucket"), config.get('gas',"Gl"), res_file)
                    table_name = config.get("gas","AnnotationsTable")
                    dynamodb = boto3.resource('dynamodb', region_name=config.get("aws", "AwsRegionName"))
                    # Get a reference to the DynamoDB table
                    table = dynamodb.Table(table_name)

                    # Update the job item in DynamoDB
                    response = table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET #s3_results_bucket = :s3_results_bucket, '
                                    '#results_file_archive_id = :results_file_archive_id',
                    ExpressionAttributeNames={
                        '#s3_results_bucket': 's3_results_bucket',
                        '#results_file_archive_id': 'results_file_archive_id'
                    },
                    ExpressionAttributeValues={
                        ':s3_results_bucket': config.get('s3', "ResultBucket"),
                        ':results_file_archive_id': gid
                    },
                    ReturnValues='UPDATED_NEW'
                    )

                    #delete s3  
                    s3 = boto3.client('s3', region_name=config.get("aws", "AwsRegionName"))
                    s3.delete_object(Bucket=config.get('s3', "ResultBucket"), Key=res_file)

                # Delete messages
                res = sqs.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=message['ReceiptHandle']
                )

      
    except Exception as e:    
        # Handle error, log, or retry logic as needed 
        print(f"Error processing message: {str(e)}")
        


def main():

    # Get handles to SQS
    sqs_client = boto3.client('sqs', region_name=config.get("aws", "AwsRegionName"))

    # Poll queue for new results and process them
    while True:
        handle_archive_queue(sqs_client)


if __name__ == "__main__":
    main()

### EOF
