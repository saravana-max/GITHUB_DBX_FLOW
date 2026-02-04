from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name = "scd1_RAW_TABLE")
def raw_table():
    df =spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").load("/Volumes/salesforce/salesforce/githup_raw_folder/").withColumn("start_dates",current_timestamp())
    return df

dp.create_streaming_table(name = "scd1_raw_table_tgt", comment = "raw target table")

dp.create_auto_cdc_flow(
    source="scd1_RAW_TABLE",
    target="scd1_raw_table_tgt",
    keys=["Empid"],
    sequence_by="start_dates",
    stored_as_scd_type=1,
    except_column_list=["_rescued_data","start_dates"]
)