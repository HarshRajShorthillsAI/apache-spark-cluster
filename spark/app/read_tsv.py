from pyspark.sql import SparkSession
import os
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.master('spark://localhost:7077').appName("ReadLargeCSV").getOrCreate()

    # Path to the large CSV file (adjust path as needed)
    file_path = "/data/*.tsv.gz"

    # Read the CSV file into a DataFrame with header and inferSchema for large data
    df = spark.read.option("header", True).option("delimiter", '\t').option("inferSchema", True).option('compression', 'gzip').csv(file_path, header=None,  mode='DROPMALFORMED')
    df = df.repartition(4)

    # Show the schema to verify data types
    df.printSchema()

    # Show the first few rows of the DataFrame
    df.show()

    # Count the number of rows in the DataFrame
    row_count = df.count()
    print(f"Total rows in the CSV file: {row_count}")

    # Stop the Spark session
    spark.stop() 