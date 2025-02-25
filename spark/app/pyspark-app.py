from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PySparkExample").getOrCreate()
    
    data = [("John", 28), ("Doe", 34), ("Jane", 29)]
    columns = ["Name", "Age"]
    
    df = spark.createDataFrame(data, columns)
    df.show()
    
    spark.stop()
