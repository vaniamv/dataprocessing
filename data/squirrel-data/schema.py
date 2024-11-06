from pyspark.sql.types import *
schema = StructType([
StructField('Area Name',StringType(),True), 
StructField('Area ID',StringType(),True), 
StructField('Park Name',StringType(),True),
StructField('Park ID', StringType(), True),
StructField('Squirrel ID', StringType(), True),
StructField('Primary Fur Color', StringType(), True),
StructField('Highlights in Fur Color', StringType(), True),
StructField('Color Notes', StringType(), True),
StructField('Location', StringType(), True),
StructField('Above Ground (Height in Feet)', StringType(), True),
StructField('Specific Location', StringType(), True),
StructField('Activities', StringType(), True),
StructField('Interactions with Humans', StringType(), True),
StructField('Squirrel Latitude (DD.DDDDDD)', StringType(), True),
StructField('Squirrel Longitude (-DD.DDDDDD)', StringType(), True)
])