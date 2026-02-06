# This script is the I/O (Input/Output) Module. It acts as the bridge between your
# local Python code and S3 Cloud Storage (or a local emulator like LocalStack). It handles
# the "dirty work" of converting binary cloud data into clean Pandas DataFrames and vice versa

from __future__ import annotations
import io
from typing import Optional

import boto3
import pandas as pd
from botocore.config import Config

def make_s3_client(endpoint_url: str, region: str, access_key: str, secret_key: str):
    """
    Creates and returns a connection to S3.
    Includes specific tweaks for 'LocalStack' (a common tool for testing S3 locally).
    """
    # config handles low-level connection rules
    cfg = Config(
        s3={"addressing_style": "path"}, # Necessary for LocalStack to recognize bucket names
        retries={"max_attempts": 5, "mode": "standard"}, # Retries failed connections
    )
    return boto3.client(
        "s3",
        # Fixes a common Windows issue where 'localhost' resolves slowly
        endpoint_url=endpoint_url.replace("localhost", "127.0.0.1"),
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=False,   # Local testing usually doesn't use HTTPS (SSL)
        verify=False,    # Skips certificate checks for local environments
        config=cfg,
    )

def join_key(prefix: str, *parts: str) -> str:
    """
    Safely joins folder names and file names into an S3 path.
    Prevents errors caused by double slashes (//) or backslashes (\).
    """
    prefix = (prefix or "").strip("/")
    clean_parts = [p.strip("/").replace("\\", "/") for p in parts if p]
    if prefix:
        return "/".join([prefix] + clean_parts)
    return "/".join(clean_parts)

def s3_read_csv(s3_client, bucket: str, key: str, **kwargs) -> pd.DataFrame:
    """
    Downloads a CSV from S3 and converts it directly into a Pandas DataFrame.
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read() # Reads raw bytes from the cloud
    # io.BytesIO makes the raw bytes look like a 'file' so Pandas can read it
    return pd.read_csv(io.BytesIO(data), **kwargs)

def s3_write_parquet(
    s3_client,
    df: pd.DataFrame,
    bucket: str,
    key: str,
    compression: str = "snappy",
    index: bool = False,
):
    """
    Converts a DataFrame to Parquet format and uploads it to S3.
    Parquet is faster and smaller than CSV for big data.
    """
    buf = io.BytesIO() # Temporary memory buffer to hold the file before uploading
    df.to_parquet(buf, engine="pyarrow", compression=compression, index=index)
    buf.seek(0) # Rewind the buffer to the beginning
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def s3_read_parquet(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """
    Downloads a Parquet file from S3 and converts it back into a Pandas DataFrame.
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")

def s3_list_objects(s3_client, bucket: str, prefix: str) -> list[str]:
    """
    Lists all files inside an S3 folder (prefix).
    Uses 'ContinuationToken' (Pagination) to ensure it gets every file 
    even if there are thousands of them.
    """
    prefix = prefix.strip("/") + "/"
    keys = []
    token = None
    
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        
        # list_objects_v2 is the standard way to browse S3 folders
        resp = s3_client.list_objects_v2(**kwargs)
        
        # Extract the file paths (Keys) from the response
        for item in resp.get("Contents", []):
            keys.append(item["Key"])
            
        # If 'IsTruncated' is true, there are more files to fetch
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys