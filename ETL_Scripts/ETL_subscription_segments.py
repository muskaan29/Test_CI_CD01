#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################
# Author - Muskaan Narang
# Date  - 2019-07-05
# Purpose -  ETL for ingesting user segments data
##############################

from os.path import expanduser, join, abspath
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func
from pyspark.sql.functions import lit, col,trim,create_map,concat
from itertools import chain

spark = SparkSession.builder.appName('Spark Sub Health ETL'
        ).config('hive.exec.dynamic.partition', 'true'
        ).config('hive.exec.dynamic.partition.mode','nonstrict'
        ).enableHiveSupport().getOrCreate()

def main():
    #Get the data from source table for most recent partition and generate MapType probabilies out of it
    try:
        default_schema = sys.argv[1]
        prod_schema = sys.argv[2]
        snapshot_date = sys.argv[3]
        
        print("Read parameters are {} {} {} ".format(default_schema,prod_schema,snapshot_date))
        print("Selecting rows from source table")
        seg_source = spark.sql("SELECT * FROM {}.svod_segmentation_master where snapshot_date='{}'".format(default_schema,snapshot_date))
        read_count=seg_source.count()
        print ("Read count is  {} ".format(read_count))

        print ("Generating the map ".format(seg_source.count()))
        final_df=seg_source.select("userid","subscription_id","segment_name",
        create_map(lit('For the Family'),col('prob_segment_0'),
            lit('Drama Watchers'),col('prob_segment_1'),
            lit('Anime Fans'),col('prob_segment_2'),
            lit('Broadcast Generalists'),col('prob_segment_3'),
            lit('Reality Watchers'),col('prob_segment_4'),
            lit('Comedy Watchers'),col('prob_segment_5'),
            lit('Exclusive / Prestige'),col('prob_segment_6'),
            lit('Content Miners / Film Buffs'),col('prob_segment_7')).alias("prob"),"snapshot_date")

        #lookup for segments
        dim_seg = spark.sql("SELECT segment_id as metadata_seg_id, segment_name as metadata_seg_name FROM {}.dim_segments".format(prod_schema))
        join_cond = [final_df.segment_name== dim_seg.metadata_seg_name]
        final_df = final_df.join(dim_seg, join_cond, 'leftouter').select(
            'userid',
            'subscription_id',
            'metadata_seg_id' as segment_id,
            'metadata_seg_name',
            'prob',
            'snapshot_date'
            )
        final_df.createOrReplaceTempView('final_df')
        print ("final_df Read count is  {} ".format(final_df.count()))

    except Exception as e:
        error_message="Error generating the final data frame: " + str(e)
        print error_message
        exit(1)
    
    try:
        #Insert overwirite most recent partition with new data
        spark.sql("""
        INSERT OVERWRITE TABLE {}.dim_subscription_segments_snapshot_test partition (snapshot_date,release_version)
            select distinct userid ,
            subscription_id ,
            segment_id,
            coalesce(metadata_seg_name,'N/A') as segment_name, 
            prob ,
            current_timestamp ,
            snapshot_date ,
            '1.0' as release_version
            from final_df
            """.format(prod_schema))

    except Exception as e:
        error_message="Error loading the data in table dim_subscription_segments_snapshot_test : " + str(e)
        print error_message
        exit(1)

main()