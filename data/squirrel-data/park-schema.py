from pyspark.sql.types import *
schema = StructType([
StructField('Area Name',StringType(),True), 
StructField('Area ID',StringType(),True), 
StructField('Park Name',StringType(),True),
StructField('Park ID', StringType(), True),
StructField('Date', DateType(), True),
StructField('Start Time', StringType(), True),
StructField('End Time', StringType(), True),
StructField('Total Time (in minutes, if available)', StringType(), True),
StructField('Park Conditions', StringType(), True),
StructField('Other Animal Sightings', StringType(), True),
StructField('Litter', StringType(), True),
StructField('Activities', StringType(), True),
StructField('Temperature & Weather', StringType(), True),
StructField('Number of Squirrels', IntegerType(), True),
StructField('Squirrel Sighter(s)', StringType(), True),
StructField('Number of Sighters', IntegerType(), True)
])
