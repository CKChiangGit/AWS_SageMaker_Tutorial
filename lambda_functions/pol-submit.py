import json
import boto3
from datetime import datetime
import random

client = boto3.client('sagemaker')

def lambda_handler(event, context):
    
    date_today = datetime.today().strftime('%Y-%m-%d')

    year = date_today[0:4]
    month = date_today[5:7]
    day = date_today[8:10]
    
    print(year,month,day)
    
    response = client.create_transform_job(
        
        # Given a batch size of 100 images and MaxPayloadInMB = 6,
        # each inference request will contain a maximum of 6MB of input data (~10, 12 images depending on image size)
        
        TransformJobName = f'{year}-{month}-{day}-object-detection-unique',
        ModelName = 'object-detection-plastic',
        MaxPayloadInMB = 100, # max input size for each image (data sample) in a batch.
        
        # Declare that the data source is S3 with 'S3prefix'
        # Give it the relevant keys with S3Uri variable
        TransformInput = {
            
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': f's3://object-detection-batch-transform/images/{year}/{month}/{day}/'
                }
                
            },
            
            'ContentType' : 'image/jpeg',
            'CompressionType': 'None',
            'SplitType': 'None'
        },
        
        # Link output to S3 bucket's "batch-output" folder
        TransformOutput = {
            'S3OutputPath': f's3://object-detection-batch-transform/batch-output/{year}/{month}/{day}',
            'AssembleWith': 'None'
        },
        
        # Specify the model's instance type and count
        TransformResources = {
            'InstanceType': 'ml.m4.xlarge',
            'InstanceCount': 1
        },
        
        # Input and output filter are JSON path expression used to select a portion of the input data to pass to the algorithm.
        # To pass the entire dataset to the algorithm, then use the default value with $.
        DataProcessing = {
            'InputFilter': '$',
            'OutputFilter': '$',
            'JoinSource': 'None'
        }
        
    )
    
    return {
        'body': response
    }