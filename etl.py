import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Loads song_data from s3 , creates the song and artist tables and loads the data back to S3 in parquet format.
    
    spark      : Sparksession
    input_data : the input file directory in S3
    output_data: the output parquet file directory in S3
    
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    print("Creating data file from song_data")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    print("Creating songs dimension table")
    
    songs_table = df['song_id','title','artist_id','year','duration']
    song_table=songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    
    print("Creating the songs_table parquet file")
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table")


    # extract columns to create artists table
    
    print("Creating artists dimension table")
    artists_table = df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artists_table=artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print("Creating the artists_table parquet file")
    artists_table.write.parquet(output_data + "artists_table")


def process_log_data(spark, input_data, output_data):
    '''
    Loads log_data from s3 processes it ,creates the users,time dimension tables and songplays table and
    loads it back to S3 in parquet format.
    
    spark      : Sparksession
    input_data : the input file directory in S3
    output_data: the output parquet file directory in S3
    
    '''
    # get filepath to log data file
    log_data =input_data + "log_data/*/*/*"

    # read log data file
    print("Creating data file from log_data...")
    df = spark.read.format('json').load(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table
    
    print("Creating users dimension table")
    users_table = df['userId','firstName','lastName','gender','level']
    users_table=users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    print("Creating the users_table parquet file")
    users_table.write.parquet(output_data + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:str(int(int(x)/1000)))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('datetime',get_datetime(df.ts))
    
    # extract columns to create time table
    print("Creating time dimension table")
    
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format("datetime","u").alias('weekday')
    )
    
    time_table=time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    print("Creating the time_table parquet file")
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table")

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    temp_df=df.join(song_df,song_df.artist_name==df.artist) 
    temp_df=temp_df.withColumn("songplay_id",monotonically_increasing_id())
    
    print("Creating songplays table")
    
    songplays_table = temp_df.select(
        col('songplay_id').alias('songplay_id'),
        col('ts').alias('start_time'),
        col('userID').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('sessionId'),
        col('location').alias('location'),
        col('userAgent').alias('userAgent'),
        col('year').alias('year'),
        month('datetime').alias('month')
    )

    # write songplays table to parquet files partitioned by year and month
    print("Creating the songplays_table parquet file")
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table")


def main():
    '''
    Extract songs and logs data from directories in S3
    perform data transformations and load into parquet files in S3.
    
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sourav-dend-udacity/project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
