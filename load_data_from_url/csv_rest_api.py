# Databricks notebook source
#load the data

import requests
import json

def Api_data(url):
    response = requests.get(url)
    json_data = json.loads(response.text)
    return json_data


url =f"https://retoolapi.dev/CaRLc9/data"

data = Api_data(url)

df = spark.createDataFrame(data)
display(df)

df.printSchema()


# define Schema
from pyspark.sql.types import StructType,StructField,StringType,LongType
rootschema = StructType([
    StructField("class",StringType(),True),
    StructField("id",LongType(),True),
    StructField("petallength",StringType(),True),
    StructField("petalwidth",StringType(),True),
    StructField("sepallength",StringType(),True),
    StructField("sepalwidth",StringType(),True),
])


