from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Creating Spark Session 
spark = SparkSession.builder.master("local").appName("data_load").getOrCreate()

# Setting up access key for Azure blob storage 
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
sc._jsc.hadoopConfiguration().set("fs.azure.account.key.springcapitalstoragerr.blob.core.windows.net", "")


def applyLatest(transactions, type):
    '''Read DataFrame, build unique ID and for the records with the same unique ID, the one with the most recent arrival timestamp is accepted.
            
    Args:
        transaction (DataFrame): Records array
        type (str): Type of transaction (Trade or Quote)
    
    Returns:
        DataFrame
    '''

    if type == "T":
    
        trade_pk = transactions.withColumn("pk", F.concat_ws("-", "trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb"))
        earliest_record =  trade_pk.groupBy("pk").agg(F.min("arrival_tm").alias("arrival_tm"))
        corrected_result = trade_pk.join(earliest_record, ["pk", "arrival_tm"])\
                .select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")
 
    elif type == "Q":
    
        quote_pk = transactions.withColumn("pk", F.concat_ws("-", "trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb"))
        earliest_record =  quote_pk.groupBy("pk").agg(F.min("arrival_tm").alias("arrival_tm"))
        corrected_result = quote_pk.join(earliest_record, ["pk", "arrival_tm"])\
                .select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")
    
    return corrected_result

# Reading Parquet files from Azure blob storage
current_date = "2020-08-06"
trade_common = spark.read.parquet(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/output_dir/{current_date}/partition=T")
quote_common = spark.read.parquet(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/output_dir/{current_date}/partition=Q")

# Selecting required fields based on transaction type
trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")
quote = quote_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

# Filtering out outdated records
trade_corrected = applyLatest(trade, "T")
trade_corrected.show()
print(trade_corrected.count())

quote_corrected = applyLatest(quote, "Q")
quote_corrected.show()
print(quote_corrected.count())



# Loading corrected data to Azure blob storage 
# trade_corrected.write.mode("overwrite").parquet("wasbs://data@springcapitalstoragerr.blob.core.windows.net/trade/trade_dt={}".format(current_date))
# quote_corrected.write.mode("overwrite").parquet("wasbs://data@springcapitalstoragerr.blob.core.windows.net/quote/quote_dt={}".format(current_date))
trade_corrected.write.mode("append").parquet("wasbs://data@springcapitalstoragerr.blob.core.windows.net/trade")
quote_corrected.write.mode("append").parquet("wasbs://data@springcapitalstoragerr.blob.core.windows.net/quote")