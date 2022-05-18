import requests
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
from pyspark.sql import SparkSession


def executeRequest(url):
    headers = {
        # "Accept": "application/json",
        "Content-Type": "application/json",
    }

    r = requests.get(url, headers=headers)

    return r.text


spark = SparkSession.builder.appName("Spotify-ETL").getOrCreate()
data = executeRequest("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")

schema = StructType([
    StructField("Results", ArrayType(
        StructType([
            StructField("Make_ID", IntegerType()),
            StructField("Make_Name", StringType())
        ])
    ))
])

udf_executeRequest = udf(executeRequest)

requestRow = Row("url")
request_df = spark.createDataFrame([requestRow("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")])

result_df = request_df.withColumn("result", udf_executeRequest(col("url"))) \
    .withColumn("jsondata", from_json(col("result"), schema=schema)) \
    .select(col("jsondata.*")) \
    .select(explode("Results").alias("res_exp")) \
    .select("res_exp.*")
result_df.printSchema()
result_df.show(truncate=120)

print("END")
