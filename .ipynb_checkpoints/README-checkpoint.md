## Sparkify

#### Introduction
The Sparkify startup has grown over the years and in this project the engineers are helping Sparkify to move their data warehouse to a 
data lake. This will allow sparkify to achieve big data reality.
We are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

#### Datasets:
we will be working with two datasets that reside in S3.

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

## Tables:
#### Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

- users - users in the app
- user_id, first_name, last_name, gender, level
- songs - songs in music database
- song_id, title, artist_id, year, duration
- artists - artists in music database
- artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

#### Scripts
- etl.py 
This contains the ETL to read data from S3 process the data in spark cluster and load the data back in parquet format in S3.
- dl.cfg
Contains AWS Credentils.

#### Authors
This project has been created by Udacity as a part of the Data Engineering Nano Degree.