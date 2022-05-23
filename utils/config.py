from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
import json


def get_token():
    with open("utils/token.json") as json_file:
        token = json.load(json_file)["token"]
    if token == "insert your spotify token here.":
        raise ValueError("Remember to change 'utils/token.json_token' to 'utils/token.json' and add your spotify token")
    else:
        return token


class Constants:
    SPOTIFY_URL = "https://api.spotify.com/v1/me/player/recently-played?"


class Schemas:
    SPOTIFY_SCHEMA = StructType([
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
