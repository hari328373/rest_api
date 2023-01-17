# Databricks notebook source
# load data from API

import requests
import json

def api_data(url):
    response = requests.get(url)
    return response.json()

# COMMAND ----------

# on board
url = f"https://jsonplaceholder.typicode.com/users"

# COMMAND ----------

a = api_data(url)

# COMMAND ----------

df = spark.createDataFrame(a)
display(df)

# COMMAND ----------

# define schema
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, MapType,IntegerType
mapCol = MapType(StringType(),StringType(),True)
rootschema = StructType([
    StructField('address', MapType(StringType(),StringType(),True)),
StructField('company', MapType(StringType(),StringType(),True)),
 StructField('email',StringType(),True),
    StructField('id',IntegerType(),True),
    StructField('phone',StringType(),True),
    StructField('username',StringType(),True),
    StructField('website',StringType(),True)])


# COMMAND ----------


