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

# Constants
USERNAME = "21lnc2vk2pdy3gr5m4zpmzeei"
TOKEN = "BQB4VJuowZJ1EuA0Wt3VBf26Cqe-MLPI5B9MSr5kIQrQypf_N6D1p9kKccRyHlmy3m31mvyrxktqIfbMLyYpy4z9yp6OX9TOAsu_7xMKNwWKkI1WvWnyZe8M3kfwI8wkA-JWSzjrW8pu-Ht_OHNvwoUfBCNN3D4Dfyrt"

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Spotify-ETL").getOrCreate()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10", headers=headers)

    data = r.json()
    elements = data["items"]

    # with open("data.json", "w") as f:
    #     f.write(r.text)

    print("END")
