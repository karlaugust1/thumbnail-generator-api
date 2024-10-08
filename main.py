import os
import boto3
import uuid
from datetime import datetime

from fastapi import FastAPI, Request
from botocore.exceptions import ClientError

ACCESS_ID = str(os.environ['ACCESS_KEY'])
ACCESS_KEY = str(os.environ['SECRET_KEY'])
bucket_name = "thumbnails-generated-from-karl"
dynamo_table = "thumbnails"
region = "us-east-2"

app = FastAPI()

s3 = boto3.client('s3',
         aws_access_key_id=ACCESS_ID,
         aws_secret_access_key= ACCESS_KEY)

dynamodb = boto3.resource('dynamodb',
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key=ACCESS_KEY,
        region_name=region
)

@app.post('/thumbnails/presigned-urls')
async def generate_presigned_url(request: Request):
    try:
        payload = await request.json()

        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': payload['file_name']
            },
            ExpiresIn=3600
        )

        return url
    except ClientError as e:
        print(e)
        raise e
    return url

@app.get('/thumbnails')
async def list_thumbnails(request: Request):
    try:
        results = dynamodb.Table(dynamo_table).scan()

        return results['Items']
    except ClientError as e:
        print(e)
        raise e
    return []

@app.post('/thumbnails')
async def create_thumbnail(request: Request):
    try:
        payload = await request.json()

        print(payload)

        item = {
                'id': str(uuid.uuid4()),
                'thumbnail_url': str(payload["thumbnail_url"]),
                'file_name': str(payload["file_name"]),
                'compressed_thumbnail_url': str(payload["compressed_thumbnail_url"]),
                'created_at': str(datetime.now()),
                'updated_at': str(datetime.now())
            }
        response = dynamodb.Table(dynamo_table).put_item(
            Item=item
        )

        return item
    except ClientError as e:
        print(e)
        raise e
    return None

@app.put('/thumbnails/{id}')
async def update_thumbnail(id, request: Request):
    try:
        payload = await request.json()

        UpdateExpression = 'SET thumbnail_url = :val1, file_name = :val2, compressed_thumbnail_url = :val3'
        ExpressionAttributeValues = {
                ':val1': str(payload["thumbnail_url"]),
                ':val2': str(payload["file_name"]),
                ':val3': str(payload["compressed_thumbnail_url"])
            }

        dynamodb.Table(dynamo_table).update_item(
            Key={
                'id': str(id)
            },
            UpdateExpression=UpdateExpression,
            ExpressionAttributeValues=ExpressionAttributeValues
        )

        return item
    except ClientError as e:
        print(e)
        raise e
    return None

@app.delete('/thumbnails/{id}')
async def delete_thumbnail(id, request: Request):
    try:
        payload = await request.json()

        dynamodb.Table(dynamo_table).delete_item(
            Key={
                'id': str(id)
            }
        )

        return None
    except ClientError as e:
        print(e)
        raise e
    return None

    