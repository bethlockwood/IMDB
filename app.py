import logging
from datetime import datetime
from stages.auth import auth
from stages.extract import extract_all
from stages.transform import transform
from stages.load import load

# Configure logging module
logging.basicConfig(filename = 'pipeline.log', encoding='utf-8', level = logging.DEBUG)

# Authenticate s3 Bucket & Postgres Database
logging.info(f'{datetime.now()} - Beginning Authentication...')
alchemyEngine, s3_resource, s3_bucket = auth('multi_config.ini')
logging.info(f'{datetime.now()} - Authentication Complete')

# Extract data from S3 Bucket, Postgres Database and Local file
logging.info(f'{datetime.now()} - Beginning Extraction...')
local_df, db_df, s3_df = extract_all('data/IMDB-Movie-Data-Local.csv'
                                    , 'IMDB_partial_data'
                                    , alchemyEngine
                                    , s3_resource
                                    , s3_bucket
                                    , 'IMDB-Movie-Data-S3.csv')
logging.info(f'{datetime.now()} - Extraction Complete')

# Merge DataFrames from all 3 data sources
logging.info(f'{datetime.now()} - Beginning Transform...')
final_df = transform(local_df
                     , db_df
                     , s3_df)
logging.info(f'{datetime.now()} - Transform Complete')

# Load final DataFrame to S3 Bucket and/or Postgres Database
logging.info(f'{datetime.now()} - Beginning Load...')
response = load(final_df
                , alchemyEngine
                , 'imdb_final'
                , s3_resource
                , s3_bucket
                , 'imdb_final.csv'
                , load_db_bool=True
                , load_s3_bool=True)
logging.info(f'{datetime.now()} - Load Complete - {response}')