import os
import shutil
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import traceback

def read_data(spark, customSchema):
    print("Starting read_data")
    
    # Mention the Bucket name inside the bucket_name variable
    bucket_name = "your-bucket-name"  # Replace with your actual bucket name
    s3_input_path = "s3://" + bucket_name + "/inputfile/loan_data.csv"
    
    # Read the CSV file from S3
    df = spark.read.csv(s3_input_path, schema=customSchema, header=True)
    
    return df

def clean_data(input_df):
    print("Starting clean_data")
    
    # Drop rows with null values
    df = input_df.dropna()
    
    # Remove duplicate rows
    df = df.dropDuplicates()
    
    # Drop rows where the 'purpose' column contains the string 'null'
    df = df.filter(F.col("purpose") != "null")
    
    return df

def s3_load_data(data, file_name):
    print("Starting s3_load_data")
    
    # Mention the bucket name inside the bucket_name variable
    bucket_name = "your-bucket-name"  # Replace with your actual bucket name
    output_path = "s3://" + bucket_name + "/output/" + file_name
    
    if data.count() != 0:
        print("Loading the data", output_path)
        # Write the output data to S3 as a CSV file
        data.coalesce(1).write.option("header", "true").csv(output_path)
    else:
        print("Empty dataframe, hence cannot save the data", output_path)

def result_1(input_df):
    print("Starting result_1")
    
    # Filter rows based on the 'purpose' column
    df = input_df.filter(F.col("purpose").isin("educational", "small_business"))
    
    # Add 'income_to_installment_ratio' column
    df = df.withColumn("income_to_installment_ratio", F.col("log_annual_inc") / F.col("installment"))
    
    # Categorize 'int_rate' into 'low', 'medium', 'high'
    df = df.withColumn("int_rate_category", 
                       F.when(F.col("int_rate") < 0.1, "low")
                        .when((F.col("int_rate") >= 0.1) & (F.col("int_rate") < 0.15), "medium")
                        .otherwise("high"))
    
    # Flag high-risk borrowers based on specified conditions
    df = df.withColumn("high_risk_borrower", 
                       F.when((F.col("dti") > 20) & (F.col("fico") < 700) & (F.col("revol_util") > 80), 1)
                        .otherwise(0))
    
    return df

def result_2(input_df):
    print("Starting result_2")
    
    # Calculate the default rate for each purpose
    df = input_df.groupBy("purpose").agg(
        (F.sum(F.when(F.col("not_fully_paid") == 1, 1).otherwise(0)) / F.count("*")).alias("default_rate")
    )
    
    # Round the default_rate to two decimal places
    df = df.withColumn("default_rate", F.round(F.col("default_rate"), 2))
    
    return df

def redshift_load_data(data):
    print("Starting redshift_load_data")
    
    if data.count() != 0:
        print("Loading the data into Redshift...")
        
        # Replace these with your actual Redshift details
        jdbcUrl = "jdbc:redshift://<redshift-cluster-url>:<port>/<db>"
        username = "<your-username>"
        password = "<your-password>"
        table_name = "<your-table-name>"
        
        # Write the data to Redshift
        data.write \
            .format("jdbc") \
            .option("url", jdbcUrl) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
    else:
        print("Empty dataframe, hence cannot load the data")

def main():
    # Main driver program to control the flow of execution
    print("Starting main function")
    
    spark = SparkSession.builder.appName("Loan Data Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define custom schema
    customSchema = StructType([
        StructField("sl_no", IntegerType(), nullable=True),
        StructField("credit_policy", IntegerType(), nullable=True),
        StructField("purpose", StringType(), nullable=True),
        StructField("int_rate", DoubleType(), nullable=True),
        StructField("installment", DoubleType(), nullable=True),
        StructField("log_annual_inc", DoubleType(), nullable=True),
        StructField("dti", DoubleType(), nullable=True),
        StructField("fico", IntegerType(), nullable=True),
        StructField("days_with_cr_line", DoubleType(), nullable=True),
        StructField("revol_bal", IntegerType(), nullable=True),
        StructField("revol_util", DoubleType(), nullable=True),
        StructField("inq_last_6mths", IntegerType(), nullable=True),
        StructField("delinq_2yrs", IntegerType(), nullable=True),
        StructField("pub_rec", IntegerType(), nullable=True),
        StructField("not_fully_paid", IntegerType(), nullable=True)
    ])
    
    # Define paths for saving data
    clean_data_path = "/cleaned_data"
    result_1_path = "/result_1"
    
    try:
        task_1 = read_data(spark, customSchema)
    except Exception as e:
        print("Error in read_data function:", e)
        traceback.print_exc()
    
    try:
        task_2 = clean_data(task_1)
    except Exception as e:
        print("Error in clean_data function:", e)
        traceback.print_exc()
    
    try:
        task_3 = result_1(task_2)
    except Exception as e:
        print("Error in result_1 function:", e)
        traceback.print_exc()
    
    try:
        task_4 = result_2(task_2)
    except Exception as e:
        print("Error in result_2 function:", e)
        traceback.print_exc()
    
    try:
        redshift_load_data(task_4)
    except Exception as e:
        print("Error while loading redshift data:", e)
        traceback.print_exc()
    
    try:
        s3_load_data(task_2, clean_data_path)
    except Exception as e:
        print("Error while loading cleaned data:", e)
        traceback.print_exc()
    
    try:
        s3_load_data(task_3, result_1_path)
    except Exception as e:
        print("Error while loading result_1 data:", e)
        traceback.print_exc()
    
    spark.stop()

if __name__ == "__main__":
    main()
