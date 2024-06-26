# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_post.html
# https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3.html
# https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
# https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
# https://stackoverflow.com/questions/47701044/sigv4-post-example-using-python
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for

from app import app, db
from decorators import authenticated, is_premium

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
def create_annotation_job_request():

    region = app.config["AWS_REGION_NAME"]

    # Parse redirect URL query parameters for S3 object info
    bucket_name = request.args.get("bucket")
    s3_key = request.args.get("key")


    id, file = s3_key.split("/")[-1].split("~")

    # Create a job item and persist it to the annotations database
    data = {
        "job_id": id,
        "user_id": session.get('primary_identity'),
        "input_file_name": file,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": int(time.time()),
        "job_status": "PENDING"
    }

    table_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(table_name)
        response = table.put_item(Item=data)
        #push into Topic
        sns_client = boto3.client('sns', region_name=region)
        # sns_topic = "zihanhu2_job_requests"
        message = json.dumps(data)

        # Publish message to SNS topic
        response = sns_client.publish(
        TopicArn=app.config["AWS_SNS_JOB_REQUEST_TOPIC"],
        Message=message
        )
        return render_template("annotate_confirm.html", job_id=id)
    except Exception as e:
        print(f"Error adding item to DynamoDB or sending post request: {e}")
        return abort(500)

        


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
def annotations_list():
    
    try:
    # Get list of annotations to display
        dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        app.logger.info(session.get('primary_identity'))
    # Define the query parameters
        query_params = {
             'IndexName':'user_id_index',
            'KeyConditionExpression':Key('user_id').eq(session.get('primary_identity'))
        }
        response = table.query(**query_params)
        items = response['Items']
        for i in items:
            i["submit_time"] = datetime.fromtimestamp(int(i["submit_time"]))
    
        return render_template("annotations.html", jobs=items)
    except Exception as e:
        return abort(500)
"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
def annotation_details(id):
    try:
        dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

        # Define the query parameters
        query_params = {
            'KeyConditionExpression': Key('job_id').eq(id)
        }
        response = table.query(**query_params)
        item = response['Items'][0]
        item["submit_time"] = datetime.fromtimestamp(int(item["submit_time"]))
        if item['user_id'] != session.get('primary_identity'):
            return abort(403)
        res_url = ""
        res_word = "download"
        s3_client = boto3.client('s3', region_name=app.config["AWS_REGION_NAME"])
        if item["job_status"] == 'COMPLETED':
            item["complete_time"] = datetime.fromtimestamp(int(item["complete_time"]))
            #check url
            if "results_file_archive_id" not in item:
                res_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': app.config["AWS_S3_RESULTS_BUCKET"], 'Key': item["s3_key_result_file"]}, ExpiresIn=3600)
            else:
                res_word = "upgrade to Premium for download" 
                res_url = app.config["PREMIUM_URL"]
        inp_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': app.config["AWS_S3_INPUTS_BUCKET"], 'Key': app.config["AWS_S3_KEY_PREFIX"] + item["user_id"] + "/" +id+"~"+item["input_file_name"]}, ExpiresIn=3600)
        return render_template("annotation.html", job=item, r_url=res_url, i_url=inp_url, word=res_word)
    except Exception as e:
        return abort(500)

"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
def annotation_log(id):
    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    query_params = {
        'KeyConditionExpression': Key('job_id').eq(id)
    }
    response = table.query(**query_params)
    item = response['Items'][0]

    s3_client = boto3.client('s3', region_name=app.config["AWS_REGION_NAME"])
    response = s3_client.get_object(Bucket=app.config["AWS_S3_RESULTS_BUCKET"], Key=item["s3_key_log_file"])
    log_content = response['Body'].read().decode('utf-8')
    
    return render_template("view_log.html", cont=log_content)


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info

        # If A15 not completed, force-upgrade user role and initiate restoration
        pass

    elif request.method == "POST":
        # Process the subscription request

        # Create a customer on Stripe

        # Subscribe customer to pricing plan

        # Update user role in accounts database

        # Update role in the session

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # ...and make sure you handle files pending archive!

        # Display confirmation page
        pass


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
