# Databricks notebook source
# load data from API

import requests
import json

def api_data(url):
    response = requests.get(url)
    json_data = json.loads(response.text)
    return response.json()


url = f"https://jsonplaceholder.typicode.com/todos"

data = api_data(url)

df = spark.createDataFrame(data)
display(df)

# store the data in dbfs

api_data = json.dumps(data)

dbutils.fs.put("dbfs:/Nested_json/api_json2.json",api_data)

# schema
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,BooleanType

rootschema = StructType([
    StructField("userId",IntegerType(),True),
    StructField("title",StringType(),True),
    StructField("id",IntegerType(),True),
    StructField("completed",BooleanType(),True)
])

# creating dataframe with Schema

df_json = spark.read.format('json').schema(rootschema).load('dbfs:/Nested_json/api_json2.json')
display(df_json)


# filter the data

df_true = df_json.where("completed==True")
df_false = df_json.where("completed==False")


