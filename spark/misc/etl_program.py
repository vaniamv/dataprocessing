from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

class ETLFlow:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def extract_from_file(self, format: str, path: str, **kwargs) -> DataFrame:
        df = self.spark.read.format(format).load(path)
        return df

    def extract_from_api(self, url: str, schema: StructType = None):
      response = requests.get(url)
      rdd = spark.sparkContext.parallelize(response.json())
      if schema:
        df = spark.read.schema(schema).json(rdd)
      else:
        df = spark.read.json(rdd)
      return df

    def load(self, df: DataFrame, format: str, path: str, **kwargs) -> None:
        df.write.mode("overwrite").format(format).save(path)

class ETLTask(ETLFlow):

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def ingestion_vehicles(self):
      vehicle_schema = StructType([StructField('bearing', IntegerType(), True),
                                  StructField('block_id', StringType(), True),
                                  StructField('current_status', StringType(), True),
                                  StructField('id', StringType(), True),
                                  StructField('lat', FloatType(), True),
                                  StructField('line_id', StringType(), True),
                                  StructField('lon', FloatType(), True),
                                  StructField('pattern_id', StringType(), True),
                                  StructField('route_id', StringType(), True),
                                  StructField('schedule_relationship', StringType(), True),
                                  StructField('shift_id', StringType(), True),
                                  StructField('speed', FloatType(), True),
                                  StructField('stop_id', StringType(), True),
                                  StructField('timestamp', TimestampType(), True),
                                  StructField('trip_id', StringType(), True)])

      df = self.extract_from_api(url="https://api.carrismetropolitana.pt/vehicles", schema=vehicle_schema)
      self.load(df=df, format="parquet", path="/content/lake/bronze/vehicles")

    def cleansing_vehicles(self):
      df = self.extract_from_file(format="parquet", path="/content/lake/bronze/vehicles")
        
      # transformations
      df = df.withColumn("new_column", lit("test"))
      df = df.drop_duplicates()

      self.load(df=df, format="parquet", path="/content/lake/silver/vehicles")

    def enrich(self):
        pass


if __name__ == '__main__':
    
    # init spark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master('local').appName('ETL Program').getOrCreate()

    print("Starting ETL program")
    etl = ETLTask(spark)
    
    # run tasks
    print("Running Task - Ingestion Vehicles")
    etl.ingestion_vehicles()

    print("Running Task - Cleansing Vehicles")
    etl.cleansing_vehicles()

    #etl.enrich()

    print("ETL program completed")