# imdb Data Engineering Project

A data pipeline that:
- Extracts data from an AWS S3 Bucket, Postgres Database and Local Directory
- Disgards duplicate titles and merges on movie title
- Loads merged Data to Postgres/S3
- Delivers a genre report to Postgres/S3
