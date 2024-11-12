from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

class ETLFlow:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def extract(self, format: str, path: str, **kwargs) -> DataFrame:
        df = self.spark.read.format(format).load(path)
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        return df

    def load(self, df: DataFrame, format: str, path: str, **kwargs) -> None:
        df.write.format(format).save(path)

class ETLTask(ETLFlow):

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def ingestion(self):
        df = self.extract(format="csv", path="/content/lake/external/file1.csv", header=True, delimiter=",")
        # no transformations
        df = df.repartition(1)
        self.load(df=df, format="csv", path="/content/lake/bronze/file1.csv", header=True, delimiter=",")

    def cleansing(self):
        df = self.extract(format="csv", path="/content/lake/bronze/file1.csv", header=True, delimiter=",")
        
        # transformations
        df = df.withColumn("new_column", lit("test"))
        df = df.drop_duplicates()

        self.load(df=df, format="parquet", path="/content/lake/silver/file1")

    def enrich(self):
        df1 = self.extract(format="parquet", path="/content/lake/silver/file1")
        df2 = self.extract(format="parquet", path="/content/lake/silver/file2") 
        
        # transformations
        df2.cache()
        df = df1.join(df2, on=["join_column"], how="inner")

        self.load(df=df, format="parquet", path="/content/lake/gold/file3")


if __name__ == '__main__':
    
    # init spark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master('local').appName('ETL Program').getOrCreate()

    etl = ETLTask(spark)
    
    # run tasks
    etl.ingestion()
    etl.cleansing()
    etl.enrich()

    print("ETL program completed")