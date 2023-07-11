import logging
from datetime import datetime
from stages.auth import auth
from stages.extract import extract_all
from stages.transform import transform
from stages.load import load
from stages.report import report

# Configure logging module
logging.basicConfig(filename = 'pipeline.log', encoding='utf-8', level = logging.DEBUG)

# Authenticate s3 Bucket & Postgres Database
logging.info(f'{datetime.now()} - Beginning Authentication...')
creds = auth('multi_config.ini')
try:
    alchemyEngine, s3_resource, s3_bucket = creds
except ValueError:
    logging.info(f'{datetime.now()} - A problem occured during Authentication - {creds}')
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
load_response = load(final_df
                , alchemyEngine
                , 'imdb_final'
                , s3_resource
                , s3_bucket
                , 'imdb_final.csv'
                , load_db_bool=True
                , load_s3_bool=True)
logging.info(f'{datetime.now()} - Load Complete - {load_response}')

# Generate Genre report and load to S3 Bucket and Postgres Database
logging.info(f'{datetime.now()} - Beginning Report Genreation...')
report_responses = report(final_df
                          , alchemyEngine
                          , s3_resource
                          , s3_bucket)
logging.info(f'{datetime.now()} - Report Complete - {report_responses}')