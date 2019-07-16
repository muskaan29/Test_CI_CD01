#!/usr/bin/python
# -*- coding: utf-8 -*-

################################################################################
# Author - Muskaan Narang
# Date  - 2019-07-10
# Purpose -  ETL for ingesting data for SVOS and LIVE in HIVE user segments data
################################################################################

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
########################################
# Function to create dataframe for SVOD
########################################
def svod(default_schema,snapshot_date):

    print("Selecting rows from source table DATASCIENCEsvod_segmentation_master")

    seg_source = spark.sql("SELECT * FROM {}.svod_segmentation_master where snapshot_date='{}'".format(default_schema,snapshot_date))

    read_count=seg_source.count()
    print ("Read count is  {} ".format(read_count))
    print ("Generating the map ".format(seg_source.count()))
    svod_df=seg_source.select("userid","subscription_id","segment_name",
    create_map(lit('For the Family'),col('prob_segment_0'),
        lit('Drama Watchers'),col('prob_segment_1'),
        lit('Anime Fans'),col('prob_segment_2'),
        lit('Broadcast Generalists'),col('prob_segment_3'),
        lit('Reality Watchers'),col('prob_segment_4'),
        lit('Comedy Watchers'),col('prob_segment_5'),
        lit('Exclusive / Prestige'),col('prob_segment_6'),
        lit('Content Miners / Film Buffs'),col('prob_segment_7')).alias("prob"),"snapshot_date")

    return svod_df
#######################################
# Function to create dataframe for LIVE
#######################################
def live(default_schema,snapshot_date):
    # live segments
    seg_live_source = spark.sql("SELECT * FROM {}.live_segmentation_master where snapshot_date='{}'".format(default_schema,snapshot_date))
    read_live_count=seg_live_source.count()
    print ("Read count is  {} ".format(read_live_count))
    print ("Generating the map ".format(seg_live_source.count()))
    live_df=seg_live_source.select("userid","subscription_id","segment_name",
    create_map(lit('SVOD Inclined'),col('prob_segment_0'),
        lit('Live FOMO'),col('prob_segment_1'),
        lit('Local and News'),col('prob_segment_2'),
        lit('Sports Fans'),col('prob_segment_3')).alias("prob"),"snapshot_date")

    return live_df

def main():
    ####################################################################################################
    #Get the data from source table for most recent partition and generate MapType probabilies out of it
    ####################################################################################################
    try:
        print(len(sys.argv))
        print(sys.argv[1])
        print(sys.argv[2])
        print(sys.argv[3])
        print(sys.argv[4])
        print(sys.argv[5])
        print(sys.argv[6])
        print(sys.argv[7])
        default_schema = sys.argv[1]
        prod_schema = sys.argv[2]
        live_snapshot_date = sys.argv[3]
        svod_snapshot_date = sys.argv[4]
        test_schema = sys.argv[5]
        svod_status = sys.argv[6]
        live_status = sys.argv[7]
        print("Read parameters are {} {} {} {} {} {} {}".format(default_schema,prod_schema,live_snapshot_date,svod_snapshot_date,test_schema,svod_status,live_status))

        ###############################################################
        # Branch for SVOD and LIVE
        # Initializing Schema to handle Union without creating dataframe
        ################################################################
        svod_df = live_df = False
        if svod_status == 'Y':
            print('Calling SVOD Function')
            svod_df = svod(default_schema,svod_snapshot_date)
    
        if live_status == 'Y':
            print('Calling LIVE Function')
            live_df = live(default_schema,live_snapshot_date)


        ###############################################################
        # Combining SVOD and LIVE
            # CASES :
                    # If ndata needs to get loaded for SVOD and LIVE
                    # If only SVOD load is needed
                    # IF only LIVE load is needed
        ###############################################################

        if svod_df and live_df:
            final_df = svod_df.union(live_df)
        elif svod_df:
            final_df = svod_df
        else:
            final_df = live_df

        #######################################
        #lookup for segments
        #######################################
        dim_seg = spark.sql("SELECT segment_id as metadata_seg_id, segment_name as metadata_seg_name,segment_type FROM {}.dim_segments_test".format(test_schema))
        join_cond = [final_df.segment_name== dim_seg.metadata_seg_name]
        final_df = final_df.join(dim_seg, join_cond, 'leftouter').select(
            'userid',
            'subscription_id',
            'metadata_seg_id',
            'metadata_seg_name',
            'prob',
            'snapshot_date',
            'segment_type'
            )
        final_df.createOrReplaceTempView('final_df')
        print ("final_df Read count is  {} ".format(final_df.count()))
    except Exception as e:
        error_message="Error generating the final data frame: " + str(e)
        print error_message
        exit(1)
    try:
        ######################################################
        #Insert overwrite most recent partition with new data
        ######################################################
        spark.sql("""
        INSERT OVERWRITE TABLE {}.dim_subscription_segments_snapshot_20170710_test partition (snapshot_date,release_version,segment_type)
            select distinct userid,
            subscription_id,
            metadata_seg_id as segment_id,
            coalesce(metadata_seg_name,'N/A') as segment_name,
            prob as segment_probabilities,
            current_timestamp,
            snapshot_date ,
            '1.0' as release_version,
            segment_type
            from final_df
            """.format(test_schema))
    except Exception as e:
        error_message="Error loading the data in table dim_subscription_segments_snapshot : " + str(e)
        print error_message
        exit(1)

main()