# import sqlalchemy
# import pandas as pd
# from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row

# Constants
TOKEN = "BQAKHD7Ag7aXKjIioO5Ytgli8bSM_zuNPG1xjgxA0fGKEgbC1ymo_RaTytRutlP4wnQqSg1TimHWDizGoOuC-pf3PDBTGP5UF1qfOZNFKrfc6-vSDWpc1UxDnbsFktnYTnJ9XfSoBZ6bz5BnuBI"
USER = "nuri"
# Generate token: https://developer.spotify.com/console/get-recently-played/?limit=10&after=&before=


def execute_request(url):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
    r = requests.get(url, headers=headers)
    return r.text


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spotify-ETL").getOrCreate()

    schema = StructType([
        StructField("items", ArrayType(
            StructType([
                StructField("track", StructType([
                    StructField("album", StructType([
                        StructField("artists", ArrayType(
                            StructType([
                                StructField("id", StringType()),
                                StructField("name", StringType())
                            ])
                        )),
                        StructField("id", StringType()),
                        StructField("name", StringType()),
                        StructField("release_date", StringType())
                    ])),
                    StructField("duration_ms", IntegerType()),
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                    StructField("popularity", IntegerType())
                ])),
                StructField("played_at", StringType())

            ])
        ))
    ])

    udf_execute_request = udf(execute_request)
    requestRow = Row("url")
    request_df = spark.createDataFrame([requestRow("https://api.spotify.com/v1/me/player/recently-played?")])

    result_df = request_df \
        .withColumn("result", udf_execute_request(col("url"))) \
        .withColumn("jsondata", from_json(col("result"), schema=schema)) \
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
