from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from read_data import ReadData
from transform_data import TransformData

class ETLPipeline:
    def __init__(self, folder_path:str):
        self.spark = SparkSession.builder.master('spark://localhost:7077').appName('ETLPipeline').getOrCreate()
        self.read_data = ReadData(folder_path=folder_path, spark_session=self.spark, verbose=True)
        self.transform_data = TransformData(self.read_data, self.spark, True)

    def task1(self):
        self.read_data.load_zipped_data_into_single_dataframe(file_extension='.tsv.gz')

    def task2(self):
        self.task1()
        self.transform_data.filter_rows_by_post_event_id('20113')
        self.transform_data.create_productlist_dataframe()

    def task3(self):
        self.task2()
        self.transform_data.create_dealer_ad_impression_count_from_postproductlist()

    def task4(self):
        self.task3()
        self.transform_data.count_impressions_groupby()

    def task5(self):
        self.task3()
        self.transform_data.get_product_model_by_evar('117')