import os

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import input_file_name, regexp_extract


if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("cricsheet-pyspark-transformations")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # PostgreSQL configuration
    postgres_url = "jdbc:postgresql://postgres:5432/cricsheet"
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }

    data_folder_path = f'{os.getenv("AIRFLOW_HOME")}/data'

    # Read Innings CSV Files with Ball-by-ball records
    df_innings = spark.read.csv('s3a://cricsheet-raw-data/innings/', header=True, inferSchema=True)

    # Read match information files
    info_schema = StructType([
        StructField("c1", StringType(), True),
        StructField("c2", StringType(), True),
        StructField("c3", StringType(), True),
        StructField("player_name", StringType(), True),
        StructField("player_key", StringType(), True)
        ])

    # Define the directory containing the CSV files
    csv_directory = "s3a://cricsheet-raw-data/info/"

    # Read all CSV files in the directory into a DataFrame
    df_info = spark.read.option("header", "false").csv(csv_directory, schema=info_schema)

    # Match_id is only found in the file name
    df_info = df_info.withColumn("filename", regexp_extract(input_file_name(), ".*/(.*)_.*", 1))

    df_info_only = df_info.filter((df_info.c1 == 'info') & \
                        ~(df_info.c2.isin('player','registry'))) \
                        .drop('player_name','player_key','c1').select('filename','c2','c3')

    # Transpose the Info dataframe
    from pyspark.sql import functions as F

    # Pivot the DataFrame
    df_pivoted = df_info_only.groupBy("filename").pivot("c2").agg(F.first("c3"))

    # Write results to PostgreSQL
    df_pivoted.write.jdbc(
        url=postgres_url,
        table="dim_matches",
        mode="overwrite",
        properties=postgres_properties,
    )

    df_innings.write.jdbc(
        url=postgres_url,
        table="fact_innings",
        mode="overwrite",
        properties=postgres_properties,
    )
