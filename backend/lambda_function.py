import json
import boto3
import uuid
from datetime import datetime

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ImageMetadataTable')
sns = boto3.client('sns')

def lambda_handler(event, context):
    # S3 Event se Bucket Name aur File Name extract karna
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    file_size = event['Records'][0]['s3']['object']['size']
    
    # Metadata taiyar karna
    metadata = {
        'ImageID': str(uuid.uuid4()),
        'FileName': file_name,
        'BucketName': bucket_name,
        'FileSizeInBytes': file_size,
        'ProcessedAt': datetime.utcnow().isoformat()
    }
    
    # DynamoDB me save karna
    table.put_item(Item=metadata)
    
    # Email bhejna
    sns.publish(
        TopicArn='arn:aws:sns:eu-north-1:564208780370:MeraAlertSystem',
        Message=f"Success! Your file {file_name} has been securely processed and stored.",
        Subject="Cloud Pipeline Notification"
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Metadata logged and Email Sent!')
    }