from stages.auth import auth
from stages.extract import extract_all
from stages.transform import transform
from stages.load import load

# Authenticate s3 Bucket & Postgres Database
alchemyEngine, s3_resource, s3_bucket = auth('multi_config.ini')

# Extract data from S3 Bucket, Postgres Database and Local file
local_df, db_df, s3_df = extract_all('data/IMDB-Movie-Data-Local.csv'
                                    , 'IMDB_partial_data'
                                    , alchemyEngine
                                    , s3_resource
                                    , s3_bucket
                                    , 'IMDB-Movie-Data-S3.csv')

# Merge DataFrames from all 3 data sources
final_df = transform(local_df
                     , db_df
                     , s3_df)

# Load final DataFrame to S3 Bucket and/or Postgres Database
response = load(final_df
                , alchemyEngine
                , 'imdb_final'
                , s3_resource
                , s3_bucket
                , 'imdb_final.csv'
                , load_db_bool=True
                , load_s3_bool=True)
print(response)

