# run.py
#
# Runs the AnnTools pipeline
#
# NOTE: This file lives on the AnnTools instance and
# replaces the default AnnTools run.py
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import sys
import time
import driver
import boto3
import os
import json
# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

"""A rudimentary timer for coarse-grained profiling
"""
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html

class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


def main():

    # Get job parameters
    input_file_name = sys.argv[1]
    # Run the AnnTools pipeline
    with Timer():
        driver.run(input_file_name, "vcf")

    try:
        s3 = boto3.client('s3')
        result_bucket = config.get("s3", "ResultsBucketName")
        localfile = sys.argv[1]
        arr = localfile.split("/")
        dir ="/".join(arr[:-1])
        id_name = arr[-1]
        userId, iad = id_name.split(":")
        id, name = ("".join(iad)).split("~")
        pre, suf = name.split(".")

        result = pre + ".annot." + suf
        results_file_key = config.get("DEFAULT", "CnetId") + '/'+userId+'/' + id + "~" + result
        res = dir + "/" +userId +":"+ id + "~" + result
        log_file_key = config.get("DEFAULT", "CnetId") +  '/'+userId+'/' + id + "~"+ name+ ".count.log"
        log_res = localfile  + ".count.log"

        # # 2. Upload the log file to S3 results bucket
        s3.upload_file(res, result_bucket, results_file_key)
        s3.upload_file(log_res, result_bucket, log_file_key)

        # update to db
        table_name = config.get("gas","AnnotationsTable")
        
        dynamodb = boto3.resource('dynamodb', region_name=config.get("aws", "AwsRegionName"))
        # Get a reference to the DynamoDB table
        table = dynamodb.Table(table_name)

        # Update the job item in DynamoDB
        completed_time = int(time.time())
        response = table.update_item(
        Key={'job_id': id},
        UpdateExpression='SET #s3_results_bucket = :s3_results_bucket, '
                        '#s3_key_result_file = :s3_key_result_file, '
                        '#s3_key_log_file = :s3_key_log_file, '
                        '#complete_time = :complete_time, '
                        '#job_status = :job_status',
        ExpressionAttributeNames={
            '#s3_results_bucket': 's3_results_bucket',
            '#s3_key_result_file': 's3_key_result_file',
            '#s3_key_log_file': 's3_key_log_file',
            '#complete_time': 'complete_time',
            '#job_status': 'job_status'
        },
        ExpressionAttributeValues={
            ':s3_results_bucket': result_bucket,
            ':s3_key_result_file': results_file_key,
            ':s3_key_log_file': log_file_key,
            ':complete_time': completed_time,
            ':job_status': "COMPLETED"
        },
        ReturnValues='UPDATED_NEW'
        )
        
        data = {
            "job_id": id,
            "user_id": userId,
            "s3_key_result_file": results_file_key,
            "complete_time": completed_time
        }
            
        sns_client = boto3.client('sns', region_name=config.get("aws", "AwsRegionName"))
        message = json.dumps(data)
        response = sns_client.publish(
            TopicArn=config.get("sns", "Sqs_res_arn"),
            Message=message
        )
    # 3. Clean up (delete) local job files
        os.remove(res)
        os.remove(log_res)
        os.remove(localfile)
    except Exception as e:
        print(f"Error adding item to DynamoDB or uploading to s3: {e}")



if __name__ == "__main__":
    main()

### EOF

