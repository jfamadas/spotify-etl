import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
from docs.config import Constants, Schemas

# Constants
USER = "josep"


def execute_request(url):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=Constants.SPOTIFY_TOKEN)
    }
    r = requests.get(url, headers=headers)
    return r.text


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spotify-ETL").getOrCreate()

    # Extract
    udf_execute_request = udf(execute_request)
    requestRow = Row("url")
    request_df = spark.createDataFrame([requestRow(Constants.SPOTIFY_URL)])

    result_df = request_df \
        .withColumn("result", udf_execute_request(col("url"))) \
        .withColumn("jsondata", from_json(col("result"), schema=Schemas.SPOTIFY_SCHEMA)) \
        .select(explode(col("jsondata.items")).alias("songs")) \
        .select(element_at(col("songs.track.album.artists"), 1).getField("id").alias("artist_id"),
                element_at(col("songs.track.album.artists"), 1).getField("name").alias("artist_name"),
                col("songs.track.album.id").alias("album_id"),
                col("songs.track.album.name").alias("album_name"),
                col("songs.track.album.release_date").alias("album_release_date"),
                col("songs.track.duration_ms").alias("song_duration_ms"),
                col("songs.track.id").alias("song_id"),
                col("songs.track.name").alias("song_name"),
                col("songs.track.popularity").alias("song_popularity"),
                col("songs.played_at"))

    result_df.write.parquet("data/" + USER + "_spotify", mode="overwrite")

    print("END")
