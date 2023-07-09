import pandas as pd
import io

def extract_all(local_fileloc, table_name, alchemyEngine, s3_resource, s3_bucket, s3_file_name):
    local_df = _extract_local(local_fileloc)
    db_df = _extract_db(table_name, alchemyEngine)
    s3_df = _extract_s3(s3_resource, s3_bucket, s3_file_name)
    return local_df, db_df, s3_df


### HELPER FUNCTIONS ###

def _extract_local(local_fileloc):
    df = pd.read_csv(local_fileloc)
    return df

def _extract_db(table_name, alchemyEngine):
    # Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect()
    # Read data from PostgreSQL database table and load into a DataFrame instance
    try:
        sql = f'select * from "{table_name}"'
        df = pd.read_sql(sql, dbConnection)
    except Exception as e:
        print(e)
    finally:
        # Close DB connection
        dbConnection.close()
    
    df.drop("index", axis=1, inplace=True)
     
    return df

def _extract_s3(s3_resource, s3_bucket, s3_file_name):
    # create an object to store a series of bytes
    bytes_io = io.BytesIO()
    # Access the S3 object, download its content, save the bytes in bytes_io
    s3_resource.Object(s3_bucket, s3_file_name).download_fileobj(bytes_io)
    # Get the bytes out of the bytes_io object
    s3_contents = bytes_io.getvalue()
    # Use BytesIO and pandas to convert bytes to a dataframe
    df = pd.read_csv(io.BytesIO(s3_contents))
    
    return df