import boto3
import pandas as pd
import io
import os
from datetime import datetime

class S3Manager:
    def __init__(self, bucket_name=None):
        """Initialize S3 client and bucket name"""
        
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name or self._get_bucket_name()
        print(f"S3Manager initialized with bucket: {self.bucket}")
    
    def _get_bucket_name(self):
        """Get bucket name from environment variable"""
        
        bucket = os.environ.get('S3_BUCKET_NAME')
        if not bucket:
            # For now, we'll use a default naming pattern
            # We'll set this up properly in the template later
            raise ValueError("S3_BUCKET_NAME environment variable not set")
        return bucket
    
    def append_to_csv(self, filename, new_data):
        """Append new data to existing CSV file in S3"""
        
        if not new_data:
            print(f"No data to append to {filename}")
            return
        
        try:
            # Read existing CSV
            existing_df = self.read_csv(filename)
            
            # Convert new data to DataFrame
            new_df = pd.DataFrame(new_data)
            
            if not existing_df.empty:
                # Append to existing data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                # Use new data as the entire dataset
                combined_df = new_df
            
            # Write back to S3
            self.write_csv(filename, combined_df)
            print(f"Appended {len(new_data)} rows to {filename}")
            
        except Exception as e:
            print(f"Error appending to {filename}: {str(e)}")
            raise
    
    def read_csv(self, filename):
        """Read CSV file from S3"""
        
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=filename)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            return df
            
        except self.s3.exceptions.NoSuchKey:
            # File doesn't exist yet, return empty DataFrame
            print(f"File {filename} doesn't exist yet, creating new")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error reading {filename}: {str(e)}")
            return pd.DataFrame()
    
    def write_csv(self, filename, dataframe):
        """Write DataFrame to CSV file in S3"""
        
        try:
            csv_buffer = io.StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print(f"Wrote {len(dataframe)} rows to {filename}")
            
        except Exception as e:
            print(f"Error writing {filename}: {str(e)}")
            raise
    
    def generate_presigned_url(self, filename, expiration=3600):
        """Generate presigned URL for downloading CSV file"""
        
        try:
            url = self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': filename},
                ExpiresIn=expiration
            )
            return url
            
        except Exception as e:
            print(f"Error generating presigned URL for {filename}: {str(e)}")
            raise
