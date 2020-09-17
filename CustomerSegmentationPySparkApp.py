'''Application to perform customer segmentation'''

# Import libraries

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import xml
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import *



APP_NAME = "Perform Aggregation"

# read file

def read_file(path):
    filename = path.split(",")
    df = spark.read.csv(filename,header = True)
    return(df)

def write_file(filepath,df):
    df.write.csv(filepath, header = True)


if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    path = sys.argv[1]          # take user input for file path
    writepath = sys.argv[2]     # take user input for output path
    df = read_file(path)        # read  the files

    # change the column name
    df_renamed = df.withColumnRenamed("UniSA_Receipt_No1","Receipt_No").\
    withColumnRenamed("UniSA_Customer_No","Customer_No")

    # extract date and month and year from df_renamed
    extract_YMD = df_renamed.select(col("*"), substring(col("Sale_Date"), 0, 4).\
    alias("year"), substring(col("Sale_Date"), 6, 2).\
    alias("month"), substring(col("Sale_Date"), 9, 2).\
    alias("date"))

    # perform Group By and Aggregation
    group_by_agg = extract_YMD.groupBy("Customer_No","month","date","year").\
    agg(countDistinct("Receipt_No").alias("Receipt_No"), sum("Item_Value").\
    alias("Item_Value"), sum("Quantity_Sold").alias("Quantity_Sold")).\
    groupBy("Customer_No","month","year").\
    agg(sum("Receipt_No").alias("Number_of_trips_per_month_per_year"),\
    mean("Item_Value").alias("Average_Item_Value"),\
    mean("Quantity_Sold").alias("Average_Quantity_Sold"))

    # store the dataframe in memory cache (default MEMEORY and DISK)
    group_by_agg.cache().count()

    # filter the dataframe to retain geninue customers which are loyal
    filter_customer = group_by_agg.\
    filter((col("Number_of_trips_per_month_per_year") <= 20) & (col("Number_of_trips_per_month_per_year") > 12))

    # write file in csv format
    write_file(writepath,filter_customer)
