import boto3
import zipfile
import io


s3_client = boto3.client('s3')
s3_bucket = 'cs121a3-prod'
s3_key = 'developer.zip'
extract_to_folder = './developer'

def download_and_unzip():
    s3_object = s3_client.get_object(Bucket = s3_bucket, Key = s3_key)
    zip_file_in_memory = io.BytesIO(s3_object['Body'].read())

    with zipfile.ZipFile(zip_file_in_memory, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
