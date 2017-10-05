from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
from pyspark.sql.types import *

# Create Context
sc = SparkContext()
sqlContext = SQLContext(sc)

# Create the RDD 
jsonRDD = sc.wholeTextFiles("./json/xt .json").map(lambda x: x[1])

# Prepare this RDD so it can be parsed by sqlContext by removing the whitespace
js = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

# Create DataFrame
df = sqlContext.read.json(js)

productsRdd = df.select("products").rdd.flatMap(lambda d: d[0])

schemaString = "brand host name system_id"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
df2 = sqlContext.createDataFrame(productsRdd, schema)
