from sqlalchemy import create_engine
import configparser
import boto3

def auth(config_file):
    # Read the credentials from my multi_config file
    config = configparser.ConfigParser()
    credentials = config.read(config_file)
    
    # Extract my database credentials
    database = config.get('postgresql', 'database')
    user = config.get('postgresql', 'user')
    password = config.get('postgresql', 'password')
    host = config.get('postgresql', 'host')
    port = config.get('postgresql', 'port')
    
    # Create SQLAlchemy object
    alchemyEngine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}', pool_recycle=3600)

    # Extract my AWS credentials
    service_name = config.get('aws_s3', 'service_name')
    region_name = config.get('aws_s3', 'region_name')
    aws_access_key_id = config.get('aws_s3', 'aws_access_key_id')
    aws_secret_access_key = config.get('aws_s3', 'aws_secret_access_key')
    s3_bucket = config.get('aws_s3', 's3_bucket')
    
    # Create s3_resource object
    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    
    return alchemyEngine, s3_resource, s3_bucket