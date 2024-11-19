from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EcommerceDataProcessing").getOrCreate()

# Load raw data
raw_data_path = "/path/to/data/raw_data/sample_sales_data.csv"
df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

# Transformation: Calculate total sales by product
processed_df = df.groupBy("product_id").agg({"quantity": "sum", "price": "sum"})

# Save processed data
output_path = "/path/to/data/output/processed_sales_summary.csv"
processed_df.write.csv(output_path, header=True)

spark.stop()
