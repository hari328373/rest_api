# Databricks notebook source
# load data from API

import requests
import json

def api_data(url):
    response = requests.get(url)
    json_data = json.loads(response.text)
    return json_data

# COMMAND ----------

# on board
url = f"https://jsonplaceholder.typicode.com/users"

# COMMAND ----------

a = api_data(url)

# COMMAND ----------

api_data = json.dumps(a)

# COMMAND ----------

df = spark.createDataFrame(a)
#display(df)

# COMMAND ----------

# define schema
df.printSchema()

# COMMAND ----------


dbutils.fs.put("dbfs:/Nested_json/api_json1.json",api_data)

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

df1 = spark.read.format('json').schema(rootschema).load('dbfs:/Nested_json/api_json1.json')
display(df1)

# COMMAND ----------

from pyspark.sql.functions import explode,col
df2 = df.select(df.address.city,df.address.zipcode,df.address.geo)
display(df2)

# COMMAND ----------

