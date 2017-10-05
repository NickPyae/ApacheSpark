from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
import pandas as pd
import util

# Create Context
sc = SparkContext()
sqlContext = SQLContext(sc)

# Create the RDD 
jsonRDD = sc.wholeTextFiles("./json/response.json").map(lambda x: x[1])

# Prepare this RDD so it can be parsed by sqlContext by removing the whitespace
js = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

# Create DataFrame
df = sqlContext.read.json(js)

# Show the content of the DataFrame
#df.show()

# Print the schema in a tree format
df.printSchema()

# asDict will Convert the nested Row into dict if set to True
productRow = df.select("products").rdd.flatMap(lambda d: d[0]).collect()[0].asDict(True)

specification = productRow['specification']

specification = util.removeNoneFromDict(specification)

print(specification)

pandaSpec = pd.DataFrame.from_records([specification])

# print(pandaSpec)

dfSpec = sqlContext.createDataFrame(pandaSpec)

dfSpec.printSchema()
dfSpec.show()


# This will work if we don't have nested objects 
# products = df.select("products").rdd.flatMap(lambda d: d[0]).collect()
# _df = sc.parallelize(products).toDF()
# _df.show()
# df1 = sqlContext.createDataFrame(products)
# df1.show()






