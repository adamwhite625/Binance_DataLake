import os
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

def create_buckets():
    # Use 'minio' as the host for internal network if running in a container, 
    # but for manual execution on host, use localhost.
    # However, since I'm running this on the host for now, I'll use localhost:9000.
    
    endpoint = "localhost:9000"
    access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
    
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    buckets = ["bronze", "silver", "gold"]
    
    for bucket in buckets:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Bucket '{bucket}' created successfully.")
        else:
            print(f"Bucket '{bucket}' already exists.")

if __name__ == "__main__":
    create_buckets()
