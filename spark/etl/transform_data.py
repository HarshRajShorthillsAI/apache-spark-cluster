from read_data import ReadData
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col
from pyspark.sql.functions import col, split, expr, when
from pyspark.sql.functions import sum as spark_sum, col
from pyspark.sql.functions import col, split, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

class TransformData:

    def __init__(self, read_data:ReadData, spark_session:SparkSession, verbose:bool):
        self.readData = read_data
        self.folder_path = self.readData.folder_path
        self.spark = spark_session
        self.schema = [StructType([
            StructField("domain", StringType(), True),
            StructField("impression_count", IntegerType(), True)]),
            StructType([
            StructField("date", StringType(), True),
            StructField("posteventlist", IntegerType(), True)]),
            ]
        self.resultant_groupby = [self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), self.schema[0]), self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), self.schema[1])]
        self.verbose = verbose

    def filter_rows_by_post_event_id(self, pattern_str: str) -> None:
        try:
            # Filtering rows where pattern_str is present in the list after splitting
            self.readData.resultant_dataframe = self.readData.resultant_dataframe.filter(
                F.array_contains(F.split(F.col("posteventlist"), ","), pattern_str)
            )

            if self.verbose:
                self.readData.analyze_dataframe(self.readData.resultant_dataframe)

        except Exception as e:
            print(f"Error filtering rows: {e}")

    def store_dataframe_as_tsv(self, filename: str = "postproductlist20113.tsv.gz") -> None:
        try:
            output_path = os.path.join(self.folder_path, filename)

            os.rmdir(output_path) if os.path.exists(output_path) else None
            
            # Writing as a single `.tsv.gz` file (coalesce(1) forces a single output file)
            self.readData.resultant_dataframe.coalesce(1).write.option("header", True) \
                .option("delimiter", "\t").csv(output_path, compression="gzip", mode="overwrite")

            print(f"Data successfully stored as TSV at: {output_path}")

        except Exception as e:
            print(f"Error storing DataFrame as TSV: {e}")

    def create_productlist_dataframe(self) -> None:
        try:
            # Ensure the column exists
            required_columns = {"postproductlist"}
            print(f"Columns are: {self.readData.resultant_dataframe.columns}")
            assert required_columns.issubset(set(self.readData.resultant_dataframe.columns)), \
                "Dataframe must contain postproductlist column"

            # Convert comma-separated string into an array
            self.readData.resultant_dataframe = self.readData.resultant_dataframe.withColumn(
                "postproductlist", split(col("postproductlist"), ",")
            )

            # Explode: Each value in the array gets its own row
            self.readData.resultant_dataframe = self.readData.resultant_dataframe.withColumn(
                "postproductlist", explode(col("postproductlist"))
            )

            # Drop unnecessary columns
            drop_columns = ["date", "column_6"]  # Ensure "5" exists before dropping
            existing_columns = set(self.readData.resultant_dataframe.columns)
            drop_columns = [col for col in drop_columns if col in existing_columns]
            self.readData.resultant_dataframe = self.readData.resultant_dataframe.drop(*drop_columns)

            if self.verbose:
                self.readData.analyze_dataframe(self.readData.resultant_dataframe)

        except Exception as e:
            print(f"Error in create_productlist_dataframe: {e}")

    def create_dealer_ad_impression_count_from_postproductlist(self) -> None:
        try:
            # Ensure postproductlist is a string and split it by ';'
            df = self.readData.resultant_dataframe.withColumn("postproductlist", split(col("postproductlist").cast("string"), ";"))

            # Extract required fields using list indexing (Spark uses `getItem()`)
            df = df.withColumn("dealer_id", col("postproductlist").getItem(0))
            df = df.withColumn("ad_id", col("postproductlist").getItem(1))

            # Extract impression_count from 4th index and handle cases where it doesn't exist
            df = df.withColumn(
                "impression_count",
                when(col("postproductlist").getItem(4).isNotNull(),
                    split(split(col("postproductlist").getItem(4), "\\|")[0], "=")[1])
                .otherwise(None)
            )

            # Extract rest_post_product_list (joining elements from index 5 onwards)
            df = df.withColumn(
                "rest_post_product_list",
                when(col("postproductlist").getItem(5).isNotNull(),
                    expr("slice(postproductlist, 6, size(postproductlist))"))  # `6` because Spark index starts at 1
                .otherwise(None)
            )

            # Extract domain from column index 4
            df = df.withColumn(
                "domain",
                when(col("link").isNotNull() & (col("link") != ""),
                    split(split(col("link"), "www\\.")[1], "\\.")[0])
                .otherwise("")
            )

            # Selecting only the required columns
            self.readData.resultant_dataframe = df.select("dealer_id", "ad_id", "impression_count", "rest_post_product_list", "domain")

        except Exception as e:
            print(e)

        if self.verbose:
            self.readData.analyze_dataframe(self.readData.resultant_dataframe)
            print("Data transformation completed successfully.")

    def count_impressions_groupby(self) -> None:
        try:
            # Ensure the dataframe exists
            if not hasattr(self.readData, 'resultant_dataframe') or self.readData.resultant_dataframe is None:
                raise ValueError("resultant_dataframe is not initialized.")
            # Ensure correct data types
            df = self.readData.resultant_dataframe.select("impression_count", "dealer_id", "domain")
            df = df.fillna({"impression_count": 0, "domain": "", "dealer_id": 0})

            self.spark.conf.set("spark.sql.adaptive.enabled", "false")

            print("groupby started")
            # Group by 'domain' and sum 'impression_count'
            grouped_count_by_domain = df.select('domain', 'impression_count').groupBy("domain").agg(spark_sum("impression_count").alias("total_impressions"))

            # Group by 'dealer_id' and sum 'impression_count'
            grouped_count_by_dealerid = df.select('dealer_id', 'impression_count').groupBy("dealer_id").agg(spark_sum("impression_count").alias("total_impressions"))
            print("groupby complete")
            # Append to existing results
            if self.resultant_groupby[0] is None or self.resultant_groupby[0].rdd.isEmpty():
                self.resultant_groupby[0] = grouped_count_by_domain
            else:
                self.resultant_groupby[0] = self.resultant_groupby[0].union(grouped_count_by_domain)

            if self.resultant_groupby[1] is None or self.resultant_groupby[1].rdd.isEmpty():
                self.resultant_groupby[1] = grouped_count_by_dealerid
            else:
                self.resultant_groupby[1] = self.resultant_groupby[1].union(grouped_count_by_dealerid)

            if self.verbose:
                print(f"Grouped impressions by domain:{grouped_count_by_domain.count()}")
                print(f"Grouped impressions by dealer_id:{grouped_count_by_dealerid.count()}")

            self.spark.conf.set("spark.sql.adaptive.enabled", "true")

        except Exception as e:
            print(e)

    def get_product_model_by_evar(self, evar: str) -> None:
        try:
            df = self.readData.resultant_dataframe.select(['rest_post_product_list'])
            
            df = df.withColumn('evar_model', split(col('rest_post_product_list')[0], "\\|"))
            df.drop('rest_post_product_list')
            df = df.withColumn("evar_entry", explode(col("evar_model")))
            df.drop('evar_model')

            df = df.withColumn('key', split(col('evar_entry'), '=')[0])
            df = df.withColumn('value', split(col('evar_entry'), '=')[1])

            self.readData.resultant_dataframe = df.filter(col('key') == evar).select('value')      
            if self.verbose:
                self.readData.analyze_dataframe(self.readData.resultant_dataframe)
            
        except Exception as e:
            print(e)