def analyzer():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import broadcast
    import datetime

    import sys
    
    sys.path.append("C:\\Users\\FBLServer\\Documents\\c\\")
    import configocp

    # Creating Spark Session 
    spark = SparkSession.builder.master("local").appName("transaction_analyzer").getOrCreate()

    # Setting up access key for Azure blob storage
    az_key = configocp.a_k
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.azure.account.key.springcapitalstoragerr.blob.core.windows.net", az_key)

    # Read trade Parquet file from Azure blob storage partition and create temp view
    current_date = "2020-08-06"
    trades = spark.read.parquet("wasbs://data@springcapitalstoragerr.blob.core.windows.net/trade")
    trades.createOrReplaceTempView("trades")

    # trades.show(trades.count(), False)
    # trades.printSchema()


    ## Create Trade Staging Table

    # Read trade DataFrame with today's date as trade_dt partition

    trades_tmp_today = spark.sql(f"""
        SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr 
        FROM trades WHERE trade_dt = '{current_date}' 
        ORDER BY symbol, event_tm
    """)

    # trades_tmp_today.show(trades_tmp_today.count(), False)
    # print(trades_tmp_today.count())

    # Calculate 30-min moving average using temp view 
    trades_tmp_today.createOrReplaceTempView("tmp_trade_moving_avg_today")

    mov_avg_today = spark.sql("""
        SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr,
            AVG(trade_pr) 
                OVER(PARTITION BY symbol ORDER BY event_tm
                RANGE BETWEEN INTERVAL 30 minute PRECEDING and CURRENT ROW
            ) AS mov_avg_pr
        FROM tmp_trade_moving_avg_today
    """)

    # mov_avg_today.show(mov_avg_today.count(), False)

    # Save temp view into Hive table for staging 
    mov_avg_today.write.saveAsTable("temp_trade_moving_avg_today")


    ## Create staging table for prior day’s last trade

    # Get previous date value as string

    date = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    prev_date = date - datetime.timedelta(1)
    prev_date_str = datetime.datetime.strftime(prev_date, "%Y-%m-%d")

    # Read trade DataFrame with prior day's date as trade_dt partition

    trades_tmp_yesterday = spark.sql(f"""
        SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr 
        FROM trades
        WHERE trade_dt = '{prev_date_str}'
        ORDER BY symbol, event_tm
    """)

    # trades_tmp_yesterday.show(trades_tmp_yesterday.count(), False)

    # Calculate last 30-min moving average per symbol per exchange from prior day's date 

    trades_tmp_yesterday.createOrReplaceTempView("tmp_trade_moving_avg_yesterday")

    last_mov_avg_yesterday = spark.sql("""
        SELECT T1.trade_dt, T2.symbol, T2.exchange, T1.event_tm, T2.max_event_seq_nb, T1.trade_pr, T1.mov_avg_pr

        FROM(    
            SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr,
                AVG(trade_pr) 
                    OVER(PARTITION BY symbol ORDER BY event_tm
                    RANGE BETWEEN INTERVAL 30 minute PRECEDING and CURRENT ROW
                ) AS mov_avg_pr
            FROM tmp_trade_moving_avg_yesterday) AS T1
        
        RIGHT JOIN(
            SELECT symbol, exchange, MAX (event_seq_nb) AS max_event_seq_nb
            FROM tmp_trade_moving_avg_yesterday 
            GROUP BY symbol, exchange) AS T2 ON T1.symbol = T2.symbol AND T1.exchange = T2.exchange AND T1.event_seq_nb = T2.max_event_seq_nb
        
        ORDER BY symbol, event_tm
        
    """)

    # last_mov_avg_yesterday.show(last_mov_avg_yesterday.count(), False)

    # Save temp view into Hive table for staging 
    last_mov_avg_yesterday.write.saveAsTable("ttmay")



    # Read today's quote Parquet file from Azure blob storage partition and create temp view
    quotes = spark.read.parquet(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/quote")
    quotes.createOrReplaceTempView("quotes")

    # Read trade DataFrame with today's date as trade_dt partition

    quotes_today = spark.sql(f"""
        SELECT * 
        FROM quotes WHERE trade_dt = '{current_date}' 
        
    """)

    quotes_today.createOrReplaceTempView("quotes_today")
    # quotes_today.show(quotes_today.count(), False)

    # Union quotes DataFrame and temp_trade_moving_avg 

    quote_union = spark.sql("""

        SELECT trade_dt, "Q" as rec_type, symbol, event_tm, event_seq_nb, exchange, bid_pr, bid_size, ask_pr, ask_size, NULL as trade_pr, NULL as mov_avg_pr
        FROM quotes_today

        UNION

        SELECT trade_dt, "T" as rec_type, symbol, event_tm, NULL, exchange, NULL, NULL, NULL, NULL, trade_pr, mov_avg_pr  
        FROM temp_trade_moving_avg_today

        ORDER BY symbol, event_tm

    """)

    quote_union.createOrReplaceTempView("quote_union")
    # quote_union.show(quote_union.count(), True)

    # Add index to support window creation

    quote_union_indexed = spark.sql("""

        SELECT  ROW_NUMBER() OVER(ORDER BY symbol, exchange, event_tm) AS row_index, *
        FROM quote_union

    """)

    quote_union_indexed.createOrReplaceTempView("quote_union_indexed")
    # quote_union_indexed.show(quote_union_indexed.count(), True)


    # Populate Latest trade_pr and mov_avg_pr for Quote records

    quote_union_update = spark.sql("""
        
        SELECT
            row_index, trade_dt, rec_type, symbol, event_tm, event_seq_nb, exchange, bid_pr, bid_size, ask_pr, ask_size, w_partition,
            FIRST_VALUE(trade_pr) OVER (PARTITION BY w_partition ORDER BY row_index) AS last_trade_pr,
            FIRST_VALUE(mov_avg_pr) OVER (PARTITION BY w_partition ORDER BY row_index) AS last_mov_avg_pr
        
        FROM (
            SELECT  *, 
                    COUNT(trade_pr) OVER (ORDER BY row_index) AS w_partition
            FROM quote_union_indexed
        ) AS T1

    """)

    quote_union_update.createOrReplaceTempView("quote_union_update")
    # quote_union_update.show(quote_union_update.count(), True)

    # Filter for Quote records

    quote_update = spark.sql("""
        SELECT  trade_dt, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
        FROM quote_union_update
        WHERE rec_type = "Q"
    """)
    quote_update.createOrReplaceTempView("quote_update")
    # quote_update.show(quote_update.count(), True)

    # Join with table temp_last_trade To Get The Prior Day Close Price

    quote_pre_final = spark.sql("""
        
        SELECT trade_dt, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr, trade_pr_l,
        CASE WHEN event_seq_nb in ('1','2','3','4','5','6','7','8','9') THEN trade_pr_l ELSE last_trade_pr END AS last_trade_pr_f,
        CASE WHEN event_seq_nb in ('1','2','3','4','5','6','7','8','9') THEN mov_avg_pr_l ELSE last_mov_avg_pr END AS last_mov_avg_pr_f
        
        FROM (
            SELECT  quote_update.*, ttmay.trade_pr AS trade_pr_l, ttmay.mov_avg_pr AS mov_avg_pr_l
            FROM quote_update
            JOIN ttmay ON quote_update.symbol = ttmay.symbol AND quote_update.exchange = ttmay.exchange)

    """)

    # quote_pre_final.show(quote_pre_final.count(), True)
    quote_pre_final.createOrReplaceTempView("quote_pre_final")

    quote_final = spark.sql("""
        
        SELECT trade_dt, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, last_trade_pr_f, last_mov_avg_pr, last_mov_avg_pr_f,
        bid_pr - trade_pr_l AS bid_pr_mv, ask_pr - trade_pr_l AS ask_pr_mv
        
        FROM quote_pre_final

    """)

    quote_final.show(quote_final.count(), True)

    quote_final.write.mode("overwrite").parquet(f"wasbs://data@springcapitalstoragerr.blob.core.windows.net/quote-trade-analytical/date={current_date}")