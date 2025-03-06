from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import size, split
import os
import inspect

class ReadData:

    def __init__(self, folder_path:str, spark_session:SparkSession, verbose:bool):
        self.folder_path = folder_path
        self.spark = spark_session
        self.schema = StructType([
            StructField("date", StringType(), True),
            StructField("posteventlist", StringType(), True),
            StructField("postproductlist", StringType(), True),
            StructField("column_4", StringType(), True),
            StructField("link", StringType(), True),
            StructField("column_6", StringType(), True)
        ])
        self.resultant_dataframe = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), self.schema)
        self.verbose = verbose

    def read_files(self, filename:str)->DataFrame:
        assert filename in os.listdir(self.folder_path), "File not found in directory path specified."
        data_frame = self.spark.read.option("header", "false") \
               .option("delimiter", "\t") \
               .option("mode", "DROPMALFORMED") \
               .option("compression", "gzip") \
               .schema(self.schema) \
               .csv(f"{self.folder_path}/{filename}")

        # data_frame = Util.rename_dataframe_cols(data_frame)

        self.analyze_dataframe(data_frame)
        return data_frame

    def list_files(self)->list[str]:
        return os.listdir(self.folder_path)

    def load_zipped_data_into_single_dataframe(self, file_extension:str)->None:

        self.resultant_dataframe = self.spark.read.option("header", False).option("delimiter", '\t').schema(self.schema).option('compression', 'gzip').csv(f"{self.folder_path}/*{file_extension}", header=None,  mode='DROPMALFORMED')

        if self.verbose:
            self.analyze_dataframe(self.resultant_dataframe)

    def analyze_dataframe(self, data:DataFrame)->None:

        print(f"In function {inspect.stack()[1].function}\nData from dataframe:\n{data.take(1)}\ndataframe shape:{data.count(), len(data.columns)}\n")