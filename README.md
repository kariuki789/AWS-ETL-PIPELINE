# AWS ETL Pipeline - Portfolio Project

A serverless ETL (Extract, Transform, Load) pipeline built with AWS services that automatically processes data uploaded to S3 using Lambda functions.

## 📋 Overview

This project demonstrates a modern ETL pipeline that:
- **Extracts** data from S3 when new files are uploaded
- **Transforms** the data using pandas (cleaning, enrichment, categorization)
- **Loads** processed data back to S3 in JSON format
- **Triggers** automatically via S3 events (no manual intervention required)

## 🏗️ Architecture

```
Daily Data → S3 Bucket → Lambda Function → Processed Data (S3)
                ↓
            (Automatic Trigger)
```

### Components Used:
- **S3 Bucket**: Data storage with partitioned structure
- **Lambda Function**: Serverless data processing
- **IAM Roles**: Secure access management
- **CloudWatch**: Logging and monitoring

## 📁 Project Structure

```
AWS-ETL-Pipeline/
├── s3_uploader.py          # Data generator and S3 uploader
├── lambda-deployment/
│   ├── lambda_function.py  # Lambda ETL function
│   └── etl-lambda.zip     # Deployment package
├── lambda_etl_function.py  # Lambda source code
└── README.md              # This file
```

## 🚀 Setup Instructions

### Prerequisites
- AWS CLI configured with appropriate credentials
- Python 3.9+ installed
- Required Python packages: `boto3`, `pandas`

### Step 1: Install Dependencies
```bash
pip install boto3 pandas
```

### Step 2: Configure AWS Credentials
```bash
aws configure
# Enter your AWS Access Key ID, Secret Key, Region (us-east-1), and format (json)
```

### Step 3: Create S3 Bucket
The data uploader will create the bucket automatically, or create manually:
```bash
aws s3 mb s3://your-portfolio-etl-bucket
```

### Step 4: Create IAM Role for Lambda
```bash
# Create trust policy
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name lambda-execution-role \
  --assume-role-policy-document file://trust-policy.json

# Attach basic Lambda execution policy
aws iam attach-role-policy \
  --role-name lambda-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create S3 access policy
cat > s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::your-portfolio-etl-bucket/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::your-portfolio-etl-bucket"
    }
  ]
}
EOF

# Create and attach S3 policy (replace YOUR-ACCOUNT-ID)
aws iam create-policy \
  --policy-name lambda-s3-access \
  --policy-document file://s3-policy.json

aws iam attach-role-policy \
  --role-name lambda-execution-role \
  --policy-arn arn:aws:iam::YOUR-ACCOUNT-ID:policy/lambda-s3-access
```

### Step 5: Create Lambda Deployment Package
```bash
# Create deployment directory
mkdir lambda-deployment
cd lambda-deployment

# Install pandas and dependencies
pip install pandas -t .

# Copy the Lambda function code (save as lambda_function.py)
# Use the code provided in the lambda_etl_function.py file

# Create deployment ZIP
zip -r etl-lambda.zip .
```

### Step 6: Deploy Lambda Function
```bash
# Deploy Lambda function (replace YOUR-ACCOUNT-ID)
aws lambda create-function \
  --function-name portfolio-etl-lambda \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR-ACCOUNT-ID:role/lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://etl-lambda.zip \
  --timeout 300 \
  --memory-size 512
```

### Step 7: Configure S3 Trigger
```bash
# Give S3 permission to invoke Lambda
aws lambda add-permission \
  --function-name portfolio-etl-lambda \
  --principal s3.amazonaws.com \
  --action lambda:InvokeFunction \
  --statement-id s3-trigger \
  --source-arn arn:aws:s3:::your-portfolio-etl-bucket

# Configure S3 bucket notification (replace YOUR-ACCOUNT-ID)
aws s3api put-bucket-notification-configuration \
  --bucket your-portfolio-etl-bucket \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "Id": "etl-trigger",
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:YOUR-ACCOUNT-ID:function:portfolio-etl-lambda",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [
              {
                "Name": "prefix",
                "Value": "raw-data/"
              }
            ]
          }
        }
      }
    ]
  }'
```

## 📊 Data Flow

### 1. Data Generation
The `s3_uploader.py` script generates sample financial transaction data:
```python
# Sample data structure
{
    "transaction_id": "TXN_20240726_0001",
    "date": "2024-07-26",
    "timestamp": "2024-07-26 08:30:15",
    "amount": 2500.00,
    "category": "salary",
    "description": "Monthly salary",
    "transaction_type": "income",
    "account": "checking",
    "location": "Online"
}
```

### 2. S3 Storage Structure
```
your-portfolio-etl-bucket/
├── raw-data/
│   └── year=2024/
│       └── month=07/
│           └── day=26/
│               └── transactions-20240726.csv
└── processed-data/
    └── year=2024/
        └── month=07/
            └── day=26/
                └── transactions-20240726.json
```

### 3. Data Transformations
The Lambda function applies these transformations:
- **Data Cleaning**: Removes null values for critical fields
- **Type Conversion**: Converts strings to appropriate data types
- **Feature Engineering**: 
  - Adds `amount_category` (small, medium, large, very_large)
  - Calculates `amount_abs` (absolute value)
  - Extracts `day_of_week`, `month`, `year`
- **Text Processing**: Cleans and capitalizes text fields
- **Metadata Addition**: Adds processing timestamp and source info

## 🧪 Testing the Pipeline

### Upload Sample Data
```bash
# Update bucket name in s3_uploader.py, then run:
python s3_uploader.py
```

### Check Results
```bash
# Verify processed data was created
aws s3 ls s3://your-portfolio-etl-bucket/processed-data/ --recursive

# Check Lambda logs
aws logs describe-log-streams \
  --log-group-name "/aws/lambda/portfolio-etl-lambda" \
  --order-by LastEventTime \
  --descending \
  --max-items 5
```

### View Lambda Logs
```bash
# Get latest log stream
LOG_STREAM=$(aws logs describe-log-streams \
  --log-group-name "/aws/lambda/portfolio-etl-lambda" \
  --order-by LastEventTime \
  --descending \
  --max-items 1 \
  --query 'logStreams[0].logStreamName' \
  --output text)

# View logs
aws logs get-log-events \
  --log-group-name "/aws/lambda/portfolio-etl-lambda" \
  --log-stream-name "$LOG_STREAM"
```

## 📈 Expected Output

### Successful Pipeline Execution:
1. ✅ **CSV uploaded** to `raw-data/` folder
2. ✅ **Lambda triggered** automatically by S3 event
3. ✅ **Data processed** and transformed
4. ✅ **JSON file created** in `processed-data/` folder
5. ✅ **Logs show success** in CloudWatch

### Sample Log Output:
```
[INFO] ETL Lambda function started
[INFO] Processing file: s3://your-portfolio-etl-bucket/raw-data/year=2024/month=07/day=26/transactions-20240726.csv
[INFO] Successfully read 75 records from S3
[INFO] Columns: ['transaction_id', 'date', 'timestamp', 'amount', 'category', 'description', 'transaction_type', 'account', 'location']
[INFO] Starting data transformations
[INFO] Removed 0 rows with missing critical data
[INFO] Transformations completed. Final record count: 75
[INFO] Redshift not configured - skipping database load
[INFO] Processed data saved to: s3://your-portfolio-etl-bucket/processed-data/year=2024/month=07/day=26/transactions-20240726.json
[INFO] Successfully processed 75 records
```

## 🔧 Configuration

### Environment Variables (Optional)
The Lambda function checks for these environment variables:
- `REDSHIFT_HOST` - Redshift cluster endpoint
- `REDSHIFT_PORT` - Redshift port (default: 5439)
- `REDSHIFT_DB` - Database name
- `REDSHIFT_USER` - Database username
- `REDSHIFT_PASSWORD` - Database password

If not configured, the pipeline runs without database loading.

### Customization Options
- **Data Generation**: Modify `s3_uploader.py` to generate different data types
- **Transformations**: Update the `transform_data()` function in Lambda
- **Storage Format**: Change output from JSON to Parquet or other formats
- **Scheduling**: Add CloudWatch Events for scheduled data generation

## 💰 Cost Estimation

For a typical portfolio project (processing ~100 files/month):
- **Lambda**: ~$0.50/month (free tier covers most usage)
- **S3**: ~$1-2/month for storage
- **CloudWatch Logs**: ~$0.50/month
- **Total**: ~$2-3/month

## 🔍 Troubleshooting

### Common Issues:

1. **Lambda not triggering**:
   - Check S3 notification configuration
   - Verify IAM permissions
   - Ensure files are uploaded to `raw-data/` folder

2. **Permission errors**:
   - Verify IAM role has S3 access
   - Check Lambda execution role

3. **Import errors in Lambda**:
   - Ensure pandas is included in deployment package
   - Check Lambda runtime version

4. **File not found errors**:
   - Verify S3 bucket name in uploader script
   - Check file path structure

### Debug Commands:
```bash
# Check Lambda function status
aws lambda get-function --function-name portfolio-etl-lambda

# Verify S3 notification
aws s3api get-bucket-notification-configuration \
  --bucket your-portfolio-etl-bucket

# Test Lambda manually
aws lambda invoke \
  --function-name portfolio-etl-lambda \
  --payload file://test-event.json \
  response.json
```

## 🔄 Next Steps

This pipeline can be extended with:
- **Database Integration**: Add Redshift, RDS, or Athena
- **Data Validation**: Implement schema validation
- **Error Handling**: Add retry logic and dead letter queues
- **Monitoring**: Set up CloudWatch alarms
- **CI/CD**: Automate deployment with GitHub Actions
- **Data Quality**: Add data profiling and quality checks

## 📝 Notes

- This is a basic implementation suitable for learning and portfolio projects
- For production use, consider adding proper error handling, monitoring, and security measures
- The pipeline processes CSV files but can be extended to handle other formats
- Database integration (Redshift/Athena) can be added when needed