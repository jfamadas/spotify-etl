from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType


class Constants:
    # Generate token: https://developer.spotify.com/console/get-recently-played/?limit=10&after=&before=
    SPOTIFY_TOKEN = "BQDKUzPk1lpltm1_utUOkOmNBGpvlo2n3H6TfWZqFYcIXb85ssT_qjuVO6rC1M6dk9MttQYKjjz_" \
                    "Vsp1ulebs2RtCC_OQreZO6n_fSj0MXnOqlHbAEZADgJ_4vVRTzRY5MGhavEkn2wrjEuE_dpEt8GhDxL2gdLMLQuY "
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
