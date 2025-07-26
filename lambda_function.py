import json
import boto3
import pandas as pd
from io import StringIO
import psycopg2
from datetime import datetime
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Main Lambda handler triggered by S3 events
    """
    logger.info("ETL Lambda function started")
    
    try:
        # Parse S3 event
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']
        
        logger.info(f"Processing file: s3://{bucket_name}/{object_key}")
        
        # Only process files from raw-data folder
        if not object_key.startswith('raw-data/'):
            logger.info("Skipping file - not in raw-data folder")
            return {
                'statusCode': 200,
                'body': json.dumps('File skipped - not in raw-data folder')
            }
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Read and transform data
        df = read_s3_data(s3_client, bucket_name, object_key)
        transformed_df = transform_data(df)
        
        # Load to Redshift (if configured)
        if is_redshift_configured():
            load_to_redshift(transformed_df, object_key)
        else:
            logger.info("Redshift not configured - skipping database load")
        
        # Save processed data back to S3
        save_processed_data(s3_client, bucket_name, transformed_df, object_key)
        
        logger.info(f"Successfully processed {len(transformed_df)} records")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'ETL pipeline completed successfully',
                'records_processed': len(transformed_df),
                'source_file': object_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'source_file': object_key if 'object_key' in locals() else 'unknown'
            })
        }


def read_s3_data(s3_client, bucket_name, object_key):
    """
    Read CSV data from S3 and return as DataFrame
    """
    try:
        # Get object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = response['Body'].read().decode('utf-8')
        
        # Read into pandas DataFrame
        df = pd.read_csv(StringIO(data))
        
        logger.info(f"Successfully read {len(df)} records from S3")
        logger.info(f"Columns: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading data from S3: {str(e)}")
        raise


def transform_data(df):
    """
    Apply data transformations
    """
    logger.info("Starting data transformations")
    
    try:
        # Create a copy to avoid modifying original
        df_transformed = df.copy()
        
        # Basic data cleaning
        initial_count = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=['transaction_id', 'amount'])
        logger.info(f"Removed {initial_count - len(df_transformed)} rows with missing critical data")
        
        # Data type conversions
        if 'amount' in df_transformed.columns:
            df_transformed['amount'] = pd.to_numeric(df_transformed['amount'], errors='coerce')
        
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        if 'timestamp' in df_transformed.columns:
            df_transformed['timestamp'] = pd.to_datetime(df_transformed['timestamp'], errors='coerce')
        
        # Add processing metadata
        df_transformed['processed_timestamp'] = datetime.now()
        df_transformed['processed_by'] = 'lambda-etl-pipeline'
        
        # Business logic transformations
        if 'amount' in df_transformed.columns:
            # Categorize transaction sizes
            df_transformed['amount_category'] = df_transformed['amount'].apply(categorize_amount)
            
            # Calculate absolute amount for analysis
            df_transformed['amount_abs'] = df_transformed['amount'].abs()
        
        # Add day of week for time-based analysis
        if 'date' in df_transformed.columns:
            df_transformed['day_of_week'] = df_transformed['date'].dt.day_name()
            df_transformed['month'] = df_transformed['date'].dt.month
            df_transformed['year'] = df_transformed['date'].dt.year
        
        # Clean text fields
        text_columns = ['description', 'category', 'location']
        for col in text_columns:
            if col in df_transformed.columns:
                df_transformed[col] = df_transformed[col].astype(str).str.strip().str.title()
        
        logger.info(f"Transformations completed. Final record count: {len(df_transformed)}")
        
        return df_transformed
        
    except Exception as e:
        logger.error(f"Error in data transformation: {str(e)}")
        raise


def categorize_amount(amount):
    """Helper function to categorize transaction amounts"""
    if pd.isna(amount):
        return 'unknown'
    
    abs_amount = abs(amount)
    if abs_amount < 25:
        return 'small'
    elif abs_amount < 100:
        return 'medium'
    elif abs_amount < 500:
        return 'large'
    else:
        return 'very_large'


def is_redshift_configured():
    """Check if Redshift environment variables are set"""
    required_vars = ['REDSHIFT_HOST', 'REDSHIFT_PORT', 'REDSHIFT_DB', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD']
    return all(os.environ.get(var) for var in required_vars)


def load_to_redshift(df, source_file):
    """
    Load transformed data to Redshift
    """
    logger.info("Loading data to Redshift")
    
    try:
        # Connection parameters from environment variables
        conn_params = {
            'host': os.environ['REDSHIFT_HOST'],
            'port': os.environ['REDSHIFT_PORT'],
            'database': os.environ['REDSHIFT_DB'],
            'user': os.environ['REDSHIFT_USER'],
            'password': os.environ['REDSHIFT_PASSWORD']
        }
        
        # Connect to Redshift
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS portfolio_transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            date DATE,
            timestamp TIMESTAMP,
            amount DECIMAL(10,2),
            amount_abs DECIMAL(10,2),
            amount_category VARCHAR(20),
            category VARCHAR(50),
            description VARCHAR(200),
            transaction_type VARCHAR(20),
            account VARCHAR(50),
            location VARCHAR(100),
            day_of_week VARCHAR(20),
            month INTEGER,
            year INTEGER,
            processed_timestamp TIMESTAMP,
            processed_by VARCHAR(50),
            source_file VARCHAR(500)
        );
        """
        
        cursor.execute(create_table_query)
        logger.info("Table created/verified")
        
        # Insert data
        insert_count = 0
        for _, row in df.iterrows():
            try:
                insert_query = """
                INSERT INTO portfolio_transactions (
                    transaction_id, date, timestamp, amount, amount_abs, amount_category,
                    category, description, transaction_type, account, location,
                    day_of_week, month, year, processed_timestamp, processed_by, source_file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    amount = EXCLUDED.amount,
                    processed_timestamp = EXCLUDED.processed_timestamp
                """
                
                cursor.execute(insert_query, (
                    row.get('transaction_id'),
                    row.get('date'),
                    row.get('timestamp'),
                    row.get('amount'),
                    row.get('amount_abs'),
                    row.get('amount_category'),
                    row.get('category'),
                    row.get('description'),
                    row.get('transaction_type'),
                    row.get('account'),
                    row.get('location'),
                    row.get('day_of_week'),
                    row.get('month'),
                    row.get('year'),
                    row.get('processed_timestamp'),
                    row.get('processed_by'),
                    source_file
                ))
                insert_count += 1
                
            except Exception as e:
                logger.warning(f"Failed to insert row {row.get('transaction_id')}: {str(e)}")
                continue
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully loaded {insert_count} records to Redshift")
        
    except Exception as e:
        logger.error(f"Error loading to Redshift: {str(e)}")
        raise


def save_processed_data(s3_client, bucket_name, df, original_key):
    """
    Save processed data back to S3 in JSON format
    """
    logger.info("Saving processed data to S3")
    
    try:
        # Create processed data key
        processed_key = original_key.replace('raw-data', 'processed-data').replace('.csv', '.json')
        
        # Convert DataFrame to JSON
        json_data = df.to_json(orient='records', date_format='iso', default_handler=str)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=processed_key,
            Body=json_data,
            ContentType='application/json',
            Metadata={
                'original_file': original_key,
                'processed_timestamp': datetime.now().isoformat(),
                'record_count': str(len(df)),
                'processing_stage': 'transformed'
            }
        )
        
        logger.info(f"Processed data saved to: s3://{bucket_name}/{processed_key}")
        
    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        raise


# Test function for local development
def test_lambda_locally():
    """
    Test the Lambda function locally with sample S3 event
    """
    sample_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "your-portfolio-etl-bucket"
                    },
                    "object": {
                        "key": "raw-data/year=2024/month=07/day=26/transactions-20240726.csv"
                    }
                }
            }
        ]
    }
    
    context = {}
    
    result = lambda_handler(sample_event, context)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    # For local testing
    test_lambda_locally()