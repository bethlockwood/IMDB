import pandas as pd
from stages.transform import transform

def test_column_names_match():
    # Arrange
    local_df = pd.read_csv('data/IMDB-Movie-Data-Local.csv')
    db_df = pd.read_csv('data/IMDB-Movie-Data-Postgres.csv')
    s3_df = pd.read_csv('data/IMDB-Movie-Data-S3.csv')
    
    expected_output = pd.read_csv('data/IMDB-Movie-Data.csv')
    
    # Act
    actual_output = transform(local_df
                     , db_df
                     , s3_df)
    
    # Assert
    assert actual_output.columns.all() == expected_output.columns.all()
    
def test_shape_match():
    # Arrange
    local_df = pd.read_csv('data/IMDB-Movie-Data-Local.csv')
    db_df = pd.read_csv('data/IMDB-Movie-Data-Postgres.csv')
    s3_df = pd.read_csv('data/IMDB-Movie-Data-S3.csv')
    
    expected_output = pd.read_csv('data/IMDB-Movie-Data.csv')
    
    # Act
    actual_output = transform(local_df
                     , db_df
                     , s3_df)
    
    # Assert
    assert actual_output.shape == expected_output.shape
    
    
def test_rating():
    # Arrange
    local_df = pd.read_csv('data/IMDB-Movie-Data-Local.csv')
    db_df = pd.read_csv('data/IMDB-Movie-Data-Postgres.csv')
    s3_df = pd.read_csv('data/IMDB-Movie-Data-S3.csv')
    
    expected_output = 8.2
    
    # Act
    merged_dfs = transform(local_df, db_df, s3_df)
    title_room = merged_dfs[merged_dfs['Title'] == 'Room']
    actual_output = title_room['Rating'].values[0]
    
    # Assert  
    assert actual_output == expected_output
  
    
def test_genre():
    # Arrange
    local_df = pd.read_csv('data/IMDB-Movie-Data-Local.csv')
    db_df = pd.read_csv('data/IMDB-Movie-Data-Postgres.csv')
    s3_df = pd.read_csv('data/IMDB-Movie-Data-S3.csv')
    
    expected_output = 'Drama'
    
    # Act
    merged_dfs = transform(local_df, db_df, s3_df)
    title_american_honey = merged_dfs[merged_dfs['Title'] == 'American Honey']
    actual_output = title_american_honey['Genre'].values[0]
    
    # Assert  
    assert actual_output == expected_output
   
    
def test_runtime_minutes():
    # Arrange
    local_df = pd.read_csv('data/IMDB-Movie-Data-Local.csv')
    db_df = pd.read_csv('data/IMDB-Movie-Data-Postgres.csv')
    s3_df = pd.read_csv('data/IMDB-Movie-Data-S3.csv')
    
    expected_output = 102
    
    # Act
    merged_dfs = transform(local_df, db_df, s3_df)
    title_big_hero_6 = merged_dfs[merged_dfs['Title'] == 'Big Hero 6']
    actual_output = title_big_hero_6['Runtime_Minutes'].values[0]
    
    # Assert  
    assert actual_output == expected_output