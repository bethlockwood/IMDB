from datetime import date
import io
from load import load_db, load_s3

def report(final_df, alchemyEngine, s3_resource, s3_bucket):
    responses = []
    
    # Genreate genre report
    report_df = _generate_genre_report(final_df)
    
    # Load the report to Postgres and S3
    db_response = load_db(report_df, alchemyEngine, 'genre_reports')
    s3_response = load_s3(report_df, s3_resource, s3_bucket, f'{date.today()}_report.csv')
    
    # Append responses for return
    responses.append([db_response,s3_response])
    
    return responses

def _generate_genre_report(df):
    # Extract the primary genre for each film
    df['Primary Genre'] = df.apply(_extract_primary_genre, axis=1)
    
    # Group the data by primary genre and calculate sum, average, and count
    genre_report = df.groupby('Primary Genre').agg(
        Sum_Revenue=('Revenue_Millions', 'sum'),
        Average_Revenue=('Revenue_Millions', 'mean'),
        Count=('Primary Genre', 'count')
    ).reset_index()
    
    # Add a rank column on the sum of the reveue
    genre_report['Rank'] = genre_report['Sum_Revenue'].rank(ascending=False)
    
    # Sort the genres based on the Rank
    genre_report = genre_report.sort_values(by='Rank', ascending=True)
    
    # Add a column with the current date
    genre_report['Report_Date'] = date.today()
    
    return genre_report

def _extract_primary_genre(row):
    # Extract the primary genre from the genre column
    genres = row['Genre'].split(',')
    
    return genres[0].strip()