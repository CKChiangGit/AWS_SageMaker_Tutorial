import json
import boto3
import os
from datetime import datetime

date_today = datetime.today().strftime('%Y-%m-%d')
year = date_today[0:4]
month = date_today[5:7]
day = date_today[8:10]

s3 = boto3.client('s3')
BUCKET = 'obj-detection-batch-transform'
FOLDER = f'batch-output/{year}/{month}/{day}'

def lambda_handler(event, context):
    
    class my_dictionary(dict):
        
        def __init__(self):
            self = dict()
        
        def add(self, key, value):
            self[key] = value
            
    dict_obj = my_dictionary()
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket = BUCKET, Prefix = FOLDER)
    
    for page in pages:
        for obj in page['Contents']:
            # Avoid looping through irrelevant .AWS/config files with short names 
            if len(obj['Key']) > 30:
                file_key = obj['Key']
                response = s3.get_object(Bucket = BUCKET, Key = file_key)
                content = response['Body']
                jsonObject = json.loads(content.read())
                detections = jsonObject['prediction']
                
                temp_arr = []
                # Loop through the detections values and append them to the temp_arr
                for det in detections:
                    #print(det)
                    (klass, score, x0, y0, x1, y1) = det 
                    if score < 0.25:
                        continue
                    arr = [klass, score, x0, y0, x1, y1]
                    temp_arr.append(arr)
                dict_obj.add(file_key, temp_arr)
                
    results = json.dumps(dict_obj, indent = 4)
    json_name = f"{year}_{month}_{day}"
    
    # Save the results to a JSON file (2025_02_11.json) in /tmp
    with open(f'/tmp/{json_name}.json', 'w') as outfile:
        outfile.write(results)
    
    file_name = f'/tmp/{year}_{month}_{day}.json'
    desired_name_s3 = f"cleansed-jsons/{year}/{month}/{day}/{year}_{month}_{day}.json"
    
    s3_resource = boto3.resource('s3')
    s3_resource.Bucket(BUCKET).upload_file(file_name, desired_name_s3)
    
    os.remove(file_name) # Remove the file from /tmp
    
    return(
        {'body': results}
        )
    
    