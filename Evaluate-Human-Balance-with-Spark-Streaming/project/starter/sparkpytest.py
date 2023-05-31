from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("SparkTest") \
        .getOrCreate()

    # Read the file from the specified path
    file_path = "file.txt"
    df = spark.read.text(file_path)

    # Perform some operations on the DataFrame
    word_count = df.selectExpr("split(value, ' ') as words") \
        .selectExpr("explode(words) as word") \
        .groupBy("word") \
        .count()

    # Show the result
    word_count.show()
    print("success!!!")

    # Stop the Spark session
    spark.stop()
