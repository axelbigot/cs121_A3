import boto3
import zipfile
import io

from index.defs import APP_DATA_DIR


# Script that runs in the deployed environment to retrieve the developer pages.
# This is the developer.zip from M1, stored in an S3 bucket.

s3_client = boto3.client('s3')
s3_bucket = 'cs121a3-prod'


def download_and_unzip(s3_key: str, extract_to_folder: str):
    """
    Downloads a .zip from S3 and extracts it.

    Args:
        s3_key: Zip file name.
        extract_to_folder: Extraction location.
    """
    s3_object = s3_client.get_object(Bucket = s3_bucket, Key = s3_key)
    zip_file_in_memory = io.BytesIO(s3_object['Body'].read())

    with zipfile.ZipFile(zip_file_in_memory, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)

def download_and_unzip_source():
    """
    Downloads developer.zip from S3 and extracts it.
    """
    download_and_unzip('developer.zip', './developer')

def download_and_unzip_prebuilt_index():
    """
    Downloads our prebuilt index from S3 and extracts it. Saves time as the index takes a while
    to build.
    """
    download_and_unzip('prebuilt.zip', str(APP_DATA_DIR))
