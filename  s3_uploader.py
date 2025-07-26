import boto3
import pandas as pd
import random
from datetime import datetime, timedelta
import json
import os
from io import StringIO

class S3DataUploader:
    def __init__(self, bucket_name, aws_profile=None):
        """
        Initialize S3 uploader
        
        Args:
            bucket_name (str): Name of S3 bucket
            aws_profile (str): AWS profile name (optional)
        """
        self.bucket_name = bucket_name
        
        # Initialize S3 client
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            self.s3_client = session.client('s3')
        else:
            self.s3_client = boto3.client('s3')
    
    def generate_sample_data(self, date=None, num_records=50):
        """
        Generate sample financial/portfolio data
        
        Args:
            date (datetime): Date for the data (default: today)
            num_records (int): Number of records to generate
        
        Returns:
            pandas.DataFrame: Generated data
        """
        if date is None:
            date = datetime.now()
        
        # Sample categories and descriptions
        income_categories = [
            ('salary', 'Monthly salary'),
            ('freelance', 'Freelance project'),
            ('investment', 'Investment returns'),
            ('bonus', 'Performance bonus')
        ]
        
        expense_categories = [
            ('food', 'Groceries'),
            ('food', 'Restaurant'),
            ('transport', 'Gas'),
            ('transport', 'Public transport'),
            ('utilities', 'Electricity bill'),
            ('utilities', 'Internet bill'),
            ('entertainment', 'Movie tickets'),
            ('entertainment', 'Streaming service'),
            ('shopping', 'Clothing'),
            ('shopping', 'Electronics'),
            ('healthcare', 'Doctor visit'),
            ('healthcare', 'Pharmacy')
        ]
        
        data = []
        
        for i in range(num_records):
            # Random transaction type (70% expenses, 30% income)
            is_income = random.random() < 0.3
            
            if is_income:
                category, description = random.choice(income_categories)
                amount = round(random.uniform(500, 5000), 2)
            else:
                category, description = random.choice(expense_categories)
                amount = -round(random.uniform(10, 500), 2)
            
            # Add some time variation within the day
            transaction_time = date + timedelta(
                hours=random.randint(6, 22),
                minutes=random.randint(0, 59)
            )
            
            data.append({
                'transaction_id': f"TXN_{date.strftime('%Y%m%d')}_{i+1:04d}",
                'date': transaction_time.strftime('%Y-%m-%d'),
                'timestamp': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
                'amount': amount,
                'category': category,
                'description': description,
                'transaction_type': 'income' if is_income else 'expense',
                'account': random.choice(['checking', 'savings', 'credit_card']),
                'location': random.choice(['Online', 'New York', 'Los Angeles', 'Chicago', 'Houston'])
            })
        
        return pd.DataFrame(data)
    
    def upload_csv_data(self, df, date=None, folder='raw-data'):
        """
        Upload DataFrame as CSV to S3 with partitioned structure
        
        Args:
            df (pandas.DataFrame): Data to upload
            date (datetime): Date for partitioning (default: today)
            folder (str): S3 folder name
        
        Returns:
            str: S3 key of uploaded file
        """
        if date is None:
            date = datetime.now()
        
        # Create partitioned key structure
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        filename = f"transactions-{date.strftime('%Y%m%d')}.csv"
        
        s3_key = f"{folder}/year={year}/month={month}/day={day}/{filename}"
        
        # Convert DataFrame to CSV string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_data,
                ContentType='text/csv',
                Metadata={
                    'upload_timestamp': datetime.now().isoformat(),
                    'record_count': str(len(df)),
                    'data_date': date.strftime('%Y-%m-%d')
                }
            )
            print(f"✅ Successfully uploaded {len(df)} records to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            print(f"❌ Error uploading to S3: {str(e)}")
            raise
    
    def upload_json_data(self, df, date=None, folder='raw-data'):
        """
        Upload DataFrame as JSON to S3
        
        Args:
            df (pandas.DataFrame): Data to upload
            date (datetime): Date for partitioning (default: today)
            folder (str): S3 folder name
        
        Returns:
            str: S3 key of uploaded file
        """
        if date is None:
            date = datetime.now()
        
        # Create partitioned key structure
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        filename = f"transactions-{date.strftime('%Y%m%d')}.json"
        
        s3_key = f"{folder}/year={year}/month={month}/day={day}/{filename}"
        
        # Convert DataFrame to JSON
        json_data = df.to_json(orient='records', date_format='iso')
        
        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                Metadata={
                    'upload_timestamp': datetime.now().isoformat(),
                    'record_count': str(len(df)),
                    'data_date': date.strftime('%Y-%m-%d')
                }
            )
            print(f"✅ Successfully uploaded {len(df)} records to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            print(f"❌ Error uploading to S3: {str(e)}")
            raise
    
    def create_bucket_if_not_exists(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"✅ Bucket '{self.bucket_name}' already exists")
        except:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                print(f"✅ Created bucket '{self.bucket_name}'")
            except Exception as e:
                print(f"❌ Error creating bucket: {str(e)}")
                raise
    
    def upload_historical_data(self, days_back=30):
        """
        Upload historical data for multiple days
        
        Args:
            days_back (int): Number of days of historical data to generate
        """
        print(f"📊 Generating {days_back} days of historical data...")
        
        for i in range(days_back):
            date = datetime.now() - timedelta(days=i)
            
            # Generate varying amounts of data (20-100 records per day)
            num_records = random.randint(20, 100)
            df = self.generate_sample_data(date=date, num_records=num_records)
            
            # Upload as CSV
            self.upload_csv_data(df, date=date)
            
            if i % 10 == 0:
                print(f"📈 Processed {i+1}/{days_back} days...")
        
        print("✅ Historical data upload complete!")
    
    def list_uploaded_files(self, prefix='raw-data'):
        """List files in the S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                print(f"\n📁 Files in s3://{self.bucket_name}/{prefix}:")
                for obj in response['Contents']:
                    size_mb = obj['Size'] / (1024 * 1024)
                    print(f"  📄 {obj['Key']} ({size_mb:.2f} MB) - {obj['LastModified']}")
            else:
                print(f"No files found in s3://{self.bucket_name}/{prefix}")
                
        except Exception as e:
            print(f"❌ Error listing files: {str(e)}")


def main():
    """Main function to demonstrate usage"""
    
    # Configuration
    BUCKET_NAME = "your-portfolio-etl-bucket"  # Change this to your bucket name
    AWS_PROFILE = None  # Set to your AWS profile name if using profiles
    
    # Initialize uploader
    uploader = S3DataUploader(BUCKET_NAME, AWS_PROFILE)
    
    print("🚀 Starting S3 Data Upload Process...")
    
    # Create bucket if needed
    uploader.create_bucket_if_not_exists()
    
    # Option 1: Upload today's data
    print("\n📊 Generating today's data...")
    today_data = uploader.generate_sample_data(num_records=75)
    print(f"Generated {len(today_data)} records for today")
    print("\nSample data:")
    print(today_data.head())
    
    # Upload as CSV
    uploader.upload_csv_data(today_data)
    
    # Option 2: Upload historical data (uncomment if needed)
    # uploader.upload_historical_data(days_back=7)
    
    # List uploaded files
    uploader.list_uploaded_files()
    
    print("\n✅ Upload process completed!")
    print(f"📈 Data uploaded to bucket: {BUCKET_NAME}")
    print("🔄 This should trigger your Lambda function if configured!")


if __name__ == "__main__":
    main()


# Example usage as a scheduled script
class DailyDataGenerator:
    """Class for scheduled daily data generation"""
    
    def __init__(self, bucket_name):
        self.uploader = S3DataUploader(bucket_name)
    
    def run_daily_upload(self):
        """Run daily data upload - can be called by cron job"""
        try:
            # Generate data for today
            data = self.uploader.generate_sample_data(num_records=random.randint(50, 150))
            
            # Upload to S3
            s3_key = self.uploader.upload_csv_data(data)
            
            print(f"✅ Daily upload successful: {s3_key}")
            return True
            
        except Exception as e:
            print(f"❌ Daily upload failed: {str(e)}")
            return False


# Cron job example (save as separate file: daily_upload.py)
"""
#!/usr/bin/env python3
from s3_uploader import DailyDataGenerator

# Run daily upload
generator = DailyDataGenerator("your-portfolio-etl-bucket")
generator.run_daily_upload()
"""