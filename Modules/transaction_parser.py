def parser():

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, DecimalType
    import json
    from datetime import datetime
    from decimal import Decimal
    import sys

    sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
    import configocp

    # Create Spark Session 
    spark = SparkSession.builder.master("local").appName("transaction_parser").getOrCreate()

    # Set up access key for Azure blob storage 
    az_key = configocp.a_k
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.azure.account.key.springcapitalstoragerr.blob.core.windows.net", az_key)

    
    def common_event(col0_val, col1_val, col2_val, col3_val, col4_val, col5_val, col6_val, col7_val, transaction, col8_val=None, col9_val=None, col10_val=None, linex=None):

        '''Read parsed RDD columns, add columns (partition and incomplete/incorrect lines) and rearrange them based on schema and transaction type.
                
        Args:
            col0 (str): Trade date (both)
            col1 (str): File timestamp (both)
            col2 (str): Event type (both)
            col3 (str): Symbol (both)
            col4 (str): Event timestamp (both)
            col5 (str): Event sequence number (both)
            col6 (str): Exchange (both)
            col7 (str): Bid price (quote) or trade price (trade)
            transaction (str): Type of transaction
            col8 (str, optional): Bid size (quote)
            col9 (str, optional): Ask price (quote)
            col10 (str, optional): Ask size (quote)
            linex (str, optional): Incomplete/incorrect line
        
        Returns:
            List
        '''
                
        if transaction == "Q":
            
            
            return [datetime.strptime(col0_val,'%Y-%m-%d'), col2_val, col3_val, col6_val, \
                    datetime.strptime(col4_val,'%Y-%m-%d %H:%M:%S.%f'), \
                    int(col5_val), datetime.strptime(col1_val,'%Y-%m-%d %H:%M:%S.%f'), \
                    None, Decimal(col7_val), int(col8_val), Decimal(col9_val), int(col10_val), "Q", ""]
        
        elif transaction == "T":
            
            return [datetime.strptime(col0_val,'%Y-%m-%d'), col2_val, col3_val, col6_val, \
                    datetime.strptime(col4_val,'%Y-%m-%d %H:%M:%S.%f'), \
                    int(col5_val), datetime.strptime(col1_val,'%Y-%m-%d %H:%M:%S.%f'), \
                    Decimal(col7_val), None, None, None, None, "T", ""]
        
        elif transaction == "B":
            
            return [None, None, None, None, None, None, None, None, None, None, None, None, "B", linex]

    
    def parse_json(line:str):

        '''Parse RDD row (read from json file) based on transaction type.
                
        Args:
            line (str): Transaction details
        
        Returns:
            event (list)
        
        Raises:
            Exception: If there are missing values or if line has less or more values than required (incosistent line)
        '''
        record = json.loads(line)
        record_type = record['event_type']
        
        try:
        
            if record_type == "Q":

                if not record["trade_dt"] or not record["file_tm"] or not record["event_type"] \
                or not record["symbol"] or not record["event_tm"] or not record["event_seq_nb"] \
                or not record["exchange"] or not record["bid_pr"] or not record["bid_size"] \
                or not record["ask_pr"] or not record["ask_size"]:            

                    raise Exception

                event = common_event(record["trade_dt"], record["file_tm"], record["event_type"],\
                        record["symbol"], record["event_tm"], record["event_seq_nb"],\
                        record["exchange"], record["bid_pr"], "Q", record["bid_size"],\
                        record["ask_pr"], record["ask_size"])
                
                return event
            
            elif record_type == "T":
                
                if not record["trade_dt"] or not record["file_tm"] or not record["event_type"] \
                or not record["symbol"] or not record["event_tm"] or not record["event_seq_nb"] \
                or not record["exchange"] or not record["price"]:            

                    raise Exception  
                
                event = common_event(record["trade_dt"], record["file_tm"], record["event_type"],\
                        record["symbol"], record["event_tm"], record["event_seq_nb"],\
                        record["exchange"], record["price"], "T")
                                
                return event
            
        except Exception:

            return common_event("","","","","","","","","B", linex = line)


    def parse_csv(line:str):

        '''Parse RDD row (read from csv file) based on transaction type.
                
        Args:
            line (str): Transaction details
        
        Returns:
            event (list)
        
        Raises:
            Exception: If there are missing values or if line has less or more values than required (incosistent line)
        '''
        record_type_pos = 2
        record = line.split(",")
        
        try:
        
            if record[record_type_pos] == "Q":
                
                if not record[0] or not record[1] or not record[2] or not record[3] or not record[4] or not record[5] or not record[6] or not record[7] or not record[8] or not record[9] or not record[10]:
                    raise Exception

                if len(record) != 11:            
                    raise Exception

                event = common_event(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], "Q", record[8], record[9], record[10])
                
                return event
            
            elif record[record_type_pos] == "T":
                
                if not record[0] or not record[1] or not record[2] or not record[3] or not record[4] or not record[5] or not record[6] or not record[7]:
                    raise Exception
                                
                if len(record) != 9:            
                    raise Exception
                
                event = common_event(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], "T")
                
                return event
            
        except Exception:

            return common_event("","","","","","","","","B", linex = line)


    # Define schema
    EventType = StructType([
        StructField("trade_dt", DateType(), True),
        StructField("rec_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("event_tm", TimestampType(), True),
        StructField("event_seq_nb", IntegerType(), True),
        StructField("arrival_tm", TimestampType(), True),
        StructField("trade_pr", DecimalType(38, 10), True),
        StructField("bid_pr", DecimalType(38, 10), True),
        StructField("bid_size", IntegerType(), True),
        StructField("ask_pr", DecimalType(38, 10), True),
        StructField("ask_size", IntegerType(), True),
        StructField("partition", StringType(), True),
        StructField("bad_line", StringType(), True),
    ])


    # Read and parse CSV file from Azure blob storage
    current_date = "2020-08-06"
    raw_csv =spark.sparkContext.textFile(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/csv/{current_date}/NYSE")
    parsed_csv = raw_csv.map(lambda line: parse_csv(line))

    # Read and parse json file from Azure blob storage
    raw_json =spark.sparkContext.textFile(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/json/{current_date}/NASDAQ")
    parsed_json = raw_json.map(lambda line: parse_json(line))

    # Read RDDs into Dataframes with defined Schema
    df_csv = spark.createDataFrame(parsed_csv, schema=EventType)
    df_json = spark.createDataFrame(parsed_json, schema=EventType)

    # Combine dataframes
    output = df_csv.union(df_json)
    # print(output.count())
    # output.printSchema()
    # output.show(output.count())


    # Load partitioned data to Azure blob storage 
    output.write.partitionBy("partition").mode("overwrite").parquet(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/output_dir/{current_date}")