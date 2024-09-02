

# Labs
```
import uuid
import boto3
import pandas as pd
from faker import Faker
from io import StringIO

# Initialize Faker and S3 client
fake = Faker()
s3_client = boto3.client('s3')

# S3 bucket and path details
bucket_name = 'XX'
s3_path = 'raw/csv/'

# Function to generate random data
def generate_people_data(num_records=100):
    data = {
        'id': [],
        'name': [],
        'age': [],
        'city': [],
        'create_ts': []
    }

    for i in range(1, num_records + 1):
        data['id'].append(i)
        data['name'].append(fake.name())
        data['age'].append(fake.random_int(min=18, max=80))
        data['city'].append(fake.city())
        data['create_ts'].append(fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'))

    return pd.DataFrame(data)

# Function to upload CSV to S3
def upload_csv_to_s3(df, bucket, s3_path):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    file_name = f"{uuid.uuid4()}.csv"
    s3_key = f"{s3_path}{file_name}"

    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Uploaded {file_name} to s3://{bucket}/{s3_key}")

# Generate data and upload to S3
people_df = generate_people_data(num_records=100)  # Adjust num_records as needed
upload_csv_to_s3(people_df, bucket_name, s3_path)

```


# SQL 
```
-- Create DB
CREATE DATABASE TEMPDB;

-- 1. Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
STORAGE_ALLOWED_LOCATIONS = ('s3://XX/raw/csv/')
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XX:role/snowflake_int_role'
ENABLED = TRUE;

-- COPY STORAGE_AWS_EXTERNAL_ID and STORAGE_AWS_IAM_USER_ARN update trust relationship
DESC integration my_s3_integration;

-- 2. Create Stage
CREATE OR REPLACE STAGE tempdb.public.people_stage
URL = 's3://XX/raw/csv/'
STORAGE_INTEGRATION = my_s3_integration;

-- 3. Create File Format
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

-- 4. Create Table
CREATE OR REPLACE TABLE tempdb.public.people (
    id INT,
    name STRING,
    age INT,
    city STRING,
    create_ts TIMESTAMP
);

-- 5. Load Data
COPY INTO tempdb.public.people
    FROM @tempdb.public.people_stage
    FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
    ON_ERROR = 'CONTINUE';



-- 6. Verify Data
SELECT * FROM tempdb.public.people;

-- 7. Drop Objects (Optional)
DROP STAGE IF EXISTS tempdb.public.people_stage;
DROP STORAGE INTEGRATION IF EXISTS my_s3_integration;
DROP FILE FORMAT my_csv_format;
TRUNCATE people;
DROP TABLE people;

DROP STORAGE INTEGRATION my_s3_integration;

```
