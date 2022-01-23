import os, sys, time
import shutil
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from datetime import datetime
from pyspark.sql.types import StringType
#from utils import   unpersist_dataframes, processing_file, filter_hiring_tweets, writing_tweets, removing_folder
import logging
import numpy as np

LOG_DIR= "/home/csgrads/kkabi004/CS226_BigData/scripts/logs_new"
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()
print('logger: ', logger)
time_stmp= datetime.now().strftime("%m%d%Y-%H%M%S")

destpath= "/home/csgrads/kkabi004/CS226_BigData/data/pre_processed_test/"
#run_ordered_file_list=["400MB.json"]
#run_ordered_file_len= len(run_ordered_file_list)
logger.addHandler(logging.FileHandler(LOG_DIR+'/file_processed_'+time_stmp+'.log', 'a'))
print = logger.info

source_path= destpath
threads= sys.argv[1]
print("threads: {}".format(threads))
print("logger: {}".format(logger))
repartition_num= 2
half_part= repartition_num #int(repartition_num/2)
#Refer: https://stackoverflow.com/questions/26562033/how-to-set-apache-spark-executor-memory
#While running in local: setting executor memory won't have any effect. "The reason for this is that the Worker "lives" within the driver JVM process \
#that you start when you start spark-shell and the default memory used for that is 512M. You can increase that by setting spark.driver.memory to something higher, for example 5g"
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \

#    .config("spark.kryoserializer.buffer.max", "800M")\
spark = SparkSession.builder.master("local["+threads+"]").appName('Twitter_preprocess')\
                            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                             .config("spark.kryoserializer.buffer.max", "1024M").getOrCreate()
                             
print('Spark session created')
print('Spark config details: {}'.format(spark.sparkContext._conf.getAll()))
print("BroadcastJoinThreshold: {}".format(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")))
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',"314572800b")
print("BroadcastJoinThreshold: {}".format(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")))

#In client mode, this property must not be set through the SparkConf directly in your application. Instead, set this through the --driver-memory command line option or in your default properties file.
print('Driver memory allocated: {}'.format(spark.sparkContext._conf.get('spark.driver.memory')))

#################### Reading file with timestamp for processing ######################

input_path= "/home/csgrads/kkabi004/CS226_BigData/data/temp_data/"
#ordered_file_list= os.listdir(input_path)
#ordered_file_list= [_ for _ in ordered_file_list if _.endswith('json')]


def order_file_list():
    file_dict= {}
    sort_time= []
    file_lists= os.listdir(input_path)
    for f in file_lists:
        start_end= f.split('.json')[0]
        
        start_epoch_time= start_end.split('_')[0] 
        #end= start_end.split('_')[0]
        
        if len(start_epoch_time.split('.')[0]) != 10:
            start_epoch_time= float(start_epoch_time)/1000
        else:
            start_epoch_time= float(start_epoch_time)
        dt = datetime.fromtimestamp(start_epoch_time)
        file_dict[f]=dt
        sort_time.append(dt)
    sorted_file_dict= {k: v for k, v in sorted(file_dict.items(), key=lambda item: item[1])}
    ordered_file_list= list(sorted_file_dict.keys())
    ordered_file_list_len= len(ordered_file_list)
    print('Final ordered file list: {}'.format(ordered_file_list))
    return ordered_file_list

def filter_func(ids, col_name):
    if col_name:
        return ids
    
def convert_org_url_tostring(colname):
    if colname:
        return colname[0]
    else:
        return None
def convert_array_tostring(colname):
    if colname:
        return str(colname)
    else:
        return None

def unpersist_dataframes():
    print('Unpersisiting {} rdds '.format(len(spark.sparkContext._jsc.getPersistentRDDs().items())))
    for (id, rdd) in spark.sparkContext._jsc.getPersistentRDDs().items():
        rdd.unpersist()
        print("Unpersisted {} rdd".format(id))
        
def processing_file(filename, spark_sess, repartition_num):
    org_tweet_stream= spark_sess.read.json(filename, multiLine= True)
    org_tweet_stream_count= org_tweet_stream.count()
    org_tweet_stream=org_tweet_stream.repartition(repartition_num).persist()
    print('Total record count: {}'.format(org_tweet_stream_count))
    print('Current partitions: {}'.format(org_tweet_stream.rdd.getNumPartitions()))

    sensitive_ids= org_tweet_stream.select('id', 'possibly_sensitive')
    #filterUDF = F.udf(lambda z: filter_func(z))
    
    sensitive_ids =sensitive_ids.rdd.map(lambda x: filter_func(x[0],x[1])).filter(lambda x: x is not None).collect()
    print('Sensitive tweets count: {}'.format(len(sensitive_ids)))

    tweet_stream= org_tweet_stream.filter(~org_tweet_stream.id.isin(sensitive_ids)).persist()
    tweet_stream_count= tweet_stream.count()
    print('org_tweets count: {} , org_tweets after sensitive tweet filtered: {}'.format(org_tweet_stream_count,tweet_stream_count))
    #print('org_tweets after sensitive tweet filtered: ',tweet_stream_count)
    
    cols_to_keep= ['id', 'created_at', 'entities', 'extended_entities', 'extended_tweet', 'favorite_count', 'quote_count', 'quoted_status', 'reply_count', 'is_quote_status', 'quoted_status_id', 'quoted_status_permalink', 'retweet_count', 'retweeted_status', 'text', 'truncated', 'user', 'possibly_sensitive']
    tweet_stream= tweet_stream.select([F.col(c).alias("samp_"+c) for c in cols_to_keep])
    
    #Convert samp_timestamp: convert org_created_at at tne end of union and join:
    tweet_stream= tweet_stream.withColumn("samp_datetime", tweet_stream["samp_created_at"])
    tweet_stream= tweet_stream.withColumn('samp_datetime', F.regexp_extract(F.col("samp_datetime"), "(\ \w+.+)", 1))
    tweet_stream= tweet_stream.withColumn('samp_datetime', F.ltrim(tweet_stream.samp_datetime))
    tweet_stream= tweet_stream.withColumn('samp_datetime', F.regexp_replace(F.col("samp_datetime"), "(\+\d+\ )", ""))
    tweet_stream= tweet_stream.withColumn('samp_datetime', to_timestamp('samp_datetime', 'MMM dd HH:mm:ss yyyy')).persist()
    
    ###########################################################
    ######################## Quoted tweets ####################
    ###########################################################
    #If its a quoted status: all fields inside of quoted_status, quoted_status_id and quoted_status_permalink is required
    #id of the original tweet: quoted_status_id == id inside quoted_status
    #condition on is_quote_status: expand quoted tweet 
    print('############################### Parsing Quoted tweets #############################')
    quoted_tweets= tweet_stream.filter(tweet_stream.samp_is_quote_status==True).persist()
    quoted_tweets= quoted_tweets.select('samp_id', 'samp_datetime', 'samp_quoted_status_id', 'samp_quoted_status', F.col('samp_quoted_status_permalink.expanded').alias('org_url'))
    quoted_tweets_count= quoted_tweets.count()
    print('Quoted tweets counts: {}'.format(quoted_tweets_count ))
    q_a= quoted_tweets.select("samp_id", "samp_datetime", 'samp_quoted_status_id',  "samp_quoted_status.*", 'org_url')
    
    ################ Sensitive quoted tweets filtering ###############
    sensitive_ids= q_a.select('samp_id', 'possibly_sensitive')
    sensitive_ids =sensitive_ids.rdd.map(lambda x: filter_func(x[0],x[1])).filter(lambda x: x is not None).collect()
    print('Sensitive tweet counts: {}'.format(len(sensitive_ids)))
    q_a= q_a.filter(~q_a.samp_id.isin(sensitive_ids)).persist()
    
    ############## Remove duplicates ##################
    q_a1= q_a.select('id', 'samp_id', 'samp_datetime', 'created_at', \
                    'display_text_range', 'possibly_sensitive', 'entities', 'extended_entities', 'extended_tweet', \
                    'favorite_count', 'is_quote_status', 'quote_count', 'quoted_status_id', \
                    'reply_count', 'retweet_count', 'retweeted', 'source', 'text', 'truncated', 'user', \
                    'org_url')
    
    q_a2= q_a1.sort(F.col("id").asc(),F.col("samp_datetime").desc())  #use shuffle --> repartition
    q_a2= q_a2.drop_duplicates(['id']).persist()
    q_a2_unique_count= q_a2.count()
    print('q_a2 unique orginal ids count: {}'.format(q_a2_unique_count))
    #print('Partitions for q_a: ',q_a2.rdd.getNumPartitions())
    #q_a2=q_a2.repartition(8)
    #print('Partitions for q_a: ',q_a2.rdd.getNumPartitions())
    
    #filter tweets based on truncated: if truncated: get hastags and text from extended tweet else consider hastags, text and url 
    #from entities. Media is always fetched from extended_entities. Url: from samp_quoted_status_permalink
    ############# Truncated quoted tweets ##########
    truncated_quote_tweets= q_a2.filter(q_a2.truncated==True) #pick extended tweet in that case
    truncated_quote_tweets_count= truncated_quote_tweets.count()
    print('q_a unique orginal ids count: {}'.format(q_a2_unique_count))
    print('truncated_quote_tweets_count: {}'.format(truncated_quote_tweets_count))
    
    cols_to_keep= ['samp_id', 'samp_datetime', 'org_url', 'id', 'created_at', 'extended_tweet', 'user', 'favorite_count', \
          'quote_count','reply_count', 'retweet_count']
    truncated_quote_tweets= truncated_quote_tweets.select([ F.col(c) if (c.startswith('samp') or c.startswith('org_')) else F.col(c).alias("org_"+c) for c in cols_to_keep ]).persist()
    
    trunc_quote_tw= truncated_quote_tweets.select("org_id", "samp_id", "samp_datetime", "org_created_at", "org_url",\
                F.col("org_extended_tweet.extended_entities.media.media_url").alias("org_media_url"), \
                F.col("org_extended_tweet.extended_entities.media.type").alias("org_media_type"), \
                F.col("org_extended_tweet.entities.urls.expanded_url").alias("org_attached_links"), \
                F.col("org_extended_tweet.full_text").alias('org_text'), \
                F.col("org_extended_tweet.entities.hashtags.text").alias("org_hashtags"), \
                F.col("org_extended_tweet.entities.user_mentions.screen_name").alias("org_user_metions"),\
                 F.col("org_user.id").alias('user_id'), F.col("org_user.name").alias('user_name'), \
                F.col("org_user.screen_name").alias('user_screen_name'), F.col("org_user.verified").alias('user_verified'),
                F.col("org_user.profile_image_url").alias('user_profile_image_url'), \
                F.col("org_user.profile_banner_url").alias('user_profile_banner_url'), \
                F.col("org_user.profile_background_image_url").alias('user_profile_background_image_url'),
                F.col("org_user.followers_count").alias('user_followers_count'),\
                F.col("org_user.friends_count").alias('user_friends_count'), 'org_favorite_count', \
                'org_quote_count', 'org_reply_count', 'org_retweet_count')
    
    #################### Untruncated Quoted tweets ##################
    untrunc_quote_tweets= q_a2.filter(q_a2.truncated==False).persist()
    untrunc_quote_tweets_count= untrunc_quote_tweets.count()
    
    print('q_a unique orginal ids count: {}'.format(q_a2_unique_count))
    print('untrunc_quote_tweets_count: {}'.format(untrunc_quote_tweets_count))
    cols_to_keep= ['samp_id', 'samp_datetime', 'org_url', 'id', 'created_at', 'text', 'entities', 'extended_entities', 'user', 'favorite_count', \
          'quote_count','reply_count', 'retweet_count']
    untrunc_quote_tweets= untrunc_quote_tweets.select([ F.col(c) if (c.startswith('samp') or c.startswith('org_')) else F.col(c).alias("org_"+c) for c in cols_to_keep ])
    
    untrunc_quote_tw= untrunc_quote_tweets.select("org_id", "samp_id", "samp_datetime", "org_created_at", "org_url", \
                F.col("org_extended_entities.media.media_url").alias("org_media_url"),\
                F.col("org_extended_entities.media.type").alias("org_media_type"), \
                F.col("org_entities.urls.expanded_url").alias("org_attached_links"), "org_text", \
                F.col("org_entities.hashtags.text").alias("org_hashtags"), \
                F.col("org_entities.user_mentions.screen_name").alias("org_user_metions"),\
                F.col("org_user.id").alias('user_id'), F.col("org_user.name").alias('user_name'), \
                F.col("org_user.screen_name").alias('user_screen_name'), F.col("org_user.verified").alias('user_verified'),\
                F.col("org_user.profile_image_url").alias('user_profile_image_url'), \
                F.col("org_user.profile_banner_url").alias('user_profile_banner_url'), \
                F.col("org_user.profile_background_image_url").alias('user_profile_background_image_url'), \
                F.col("org_user.followers_count").alias('user_followers_count'),\
                F.col("org_user.friends_count").alias('user_friends_count'), 'org_favorite_count', \
                'org_quote_count', 'org_reply_count', 'org_retweet_count')
    
    df_quoted_concat = trunc_quote_tw.union(untrunc_quote_tw)
    df_quoted_concat= df_quoted_concat.repartition(half_part).persist()
    
    ##############################################################
    ######################## Retweeted tweets ####################
    ##############################################################
    print('############################### Parsing Retweet tweets #############################')
    #Filtering all tweets that is not a quoted tweet
    quoted_ids_list= list(quoted_tweets.select('samp_id').toPandas()['samp_id'])
    
    org_retweet_tweets= tweet_stream.filter(~tweet_stream.samp_id.isin(quoted_ids_list)).persist()
    org_retweet_tweets_count= org_retweet_tweets.count()
    print('org_retweet_tweets_count: {}'.format(org_retweet_tweets_count))
    
    temp_df= org_retweet_tweets.select('samp_id', 'samp_retweeted_status')
    retweets_ids =temp_df.rdd.map(lambda x: filter_func(x[0],x[1])).filter(lambda x: x is not None).collect() #action Job 19
    
    retweet_tweets= org_retweet_tweets.filter(org_retweet_tweets.samp_id.isin(retweets_ids)).persist()
    retweet_tweets= retweet_tweets.select('samp_id', 'samp_entities', 'samp_extended_entities', 'samp_extended_tweet', 'samp_is_quote_status', 'samp_datetime', 'samp_retweeted_status.*')
    final_retweet_tweets_count= retweet_tweets.count()
    print('final_retweet_tweets_count: {}'.format(final_retweet_tweets_count))
    
    ################ Sensitive retweets filtering ###############
    sensitive_ids= retweet_tweets.select('samp_id', 'possibly_sensitive')
    sensitive_ids =sensitive_ids.rdd.map(lambda x: filter_func(x[0],x[1])).filter(lambda x: x is not None).collect()
    print('Sensitive tweet counts: {}'.format(len(sensitive_ids)))
    
    retweet_tweets= retweet_tweets.filter(~retweet_tweets.samp_id.isin(sensitive_ids)).persist()
    retweet_tweets_count_after_Sensitive_filtered= retweet_tweets.count()
    print('retweet_tweets_count_after_Sensitive_filtered: {}'.format(retweet_tweets_count_after_Sensitive_filtered))
    
    ############## Remove duplicates ##################
    retweet_tweets= retweet_tweets.sort(F.col("id").asc(),F.col("samp_datetime").desc())
    #retweet_tweets.coalesce(1).write.format('json').save('/home/csgrads/kkabi004/CS226_BigData/data/test/retweet_tweets9.json')
    retweet_tweets= retweet_tweets.drop_duplicates(['id'])
    print('Retweets after Duplicates removed: {}'.format(retweet_tweets.count()))
    #print('Partions for retweet: ', retweet_tweets.rdd.getNumPartitions())
    #retweet_tweets=retweet_tweets.repartition(8)
    #print('Partions for retweet: ', retweet_tweets.rdd.getNumPartitions())
    
    ################ Truncated Retweet tweets #######################
    truncated_retweet_tweets= retweet_tweets.filter(retweet_tweets.truncated==True).persist() #pick extended tweet in that case
    truncated_retweet_tweets_count= truncated_retweet_tweets.count()
    print('truncated_retweet_tweets count: {}'.format( truncated_retweet_tweets_count))
    
    cols_to_keep= ['samp_id', 'samp_datetime', 'id', 'created_at', 'extended_tweet', 'user', 'favorite_count', \
          'quote_count','reply_count', 'retweet_count']
    truncated_retweet_tweets= truncated_retweet_tweets.select([ F.col(c) if (c.startswith('samp') or c.startswith('org_')) else F.col(c).alias("org_"+c) for c in cols_to_keep ])
    
    trunc_retweet_tw= truncated_retweet_tweets.select("org_id", "samp_id", "samp_datetime", "org_created_at", \
                F.col("org_extended_tweet.extended_entities.media.expanded_url").alias("org_url"),\
                F.col("org_extended_tweet.extended_entities.media.media_url").alias("org_media_url"), \
                F.col("org_extended_tweet.extended_entities.media.type").alias("org_media_type"), \
                F.col("org_extended_tweet.entities.urls.expanded_url").alias("org_attached_links"), \
                F.col("org_extended_tweet.full_text").alias('org_text'), \
                F.col("org_extended_tweet.entities.hashtags.text").alias("org_hashtags"), \
                F.col("org_extended_tweet.entities.user_mentions.screen_name").alias("org_user_metions"),\
                 F.col("org_user.id").alias('user_id'), F.col("org_user.name").alias('user_name'), \
                F.col("org_user.screen_name").alias('user_screen_name'), F.col("org_user.verified").alias('user_verified'),
                F.col("org_user.profile_image_url").alias('user_profile_image_url'), \
                F.col("org_user.profile_banner_url").alias('user_profile_banner_url'), \
                F.col("org_user.profile_background_image_url").alias('user_profile_background_image_url'),
                F.col("org_user.followers_count").alias('user_followers_count'),\
                F.col("org_user.friends_count").alias('user_friends_count'), 'org_favorite_count', \
                'org_quote_count', 'org_reply_count', 'org_retweet_count')
    ############### Untruncated Retweet tweets #######################
    untrunc_retweet_tweets= retweet_tweets.filter(retweet_tweets.truncated==False).persist() #pick extended tweet in that case
    untrunc_retweet_tweets_count= untrunc_retweet_tweets.count()
    print('untrunc_retweet_tweets count:{}'.format(untrunc_retweet_tweets_count))
    
    cols_to_keep= ['samp_id', 'samp_datetime', 'id', 'created_at', 'text', 'entities', 'extended_entities', 'user', 'favorite_count', \
          'quote_count','reply_count', 'retweet_count']
    untrunc_retweet_tweets= untrunc_retweet_tweets.select([ F.col(c) if (c.startswith('samp') or c.startswith('org_')) else F.col(c).alias("org_"+c) for c in cols_to_keep ])
    
    untrunc_retweet_tw= untrunc_retweet_tweets.select("org_id", "samp_id", "samp_datetime", "org_created_at",\
                F.col("org_extended_entities.media.expanded_url").alias("org_url"),\
                F.col("org_extended_entities.media.media_url").alias("org_media_url"), \
                F.col("org_extended_entities.media.type").alias("org_media_type"), \
                F.col("org_entities.urls.expanded_url").alias("org_attached_links"), "org_text",\
                F.col("org_entities.hashtags.text").alias("org_hashtags"), \
                F.col("org_entities.user_mentions.screen_name").alias("org_user_metions"),\
                F.col("org_user.id").alias('user_id'), F.col("org_user.name").alias('user_name'), \
                F.col("org_user.screen_name").alias('user_screen_name'), F.col("org_user.verified").alias('user_verified'),
                F.col("org_user.profile_image_url").alias('user_profile_image_url'), \
                F.col("org_user.profile_banner_url").alias('user_profile_banner_url'), \
                F.col("org_user.profile_background_image_url").alias('user_profile_background_image_url'),
                F.col("org_user.followers_count").alias('user_followers_count'),\
                F.col("org_user.friends_count").alias('user_friends_count'), 'org_favorite_count', \
                'org_quote_count', 'org_reply_count', 'org_retweet_count')
    
        
    """ Converting function to UDF """
    convertUDF = F.udf(lambda z: convert_org_url_tostring(z))
    
    #Merge both truncated and untruncated and check for duplicate org_id:
    df_retweet_concat = trunc_retweet_tw.union(untrunc_retweet_tw)
    df_retweet_concat_count= df_retweet_concat.count()
    df_retweet_concat= df_retweet_concat.withColumn('org_url', convertUDF(F.col('org_url')))
    df_retweet_concat= df_retweet_concat.repartition(half_part).persist()
    
    ##############################################################
    ######################## Original tweets ####################
    ##############################################################
    
    #unpersists df:
    print("Unpersisting datafrmes")
    #time.sleep(10)
    #org_tweet_stream.unpersist()  #blocking = True
    #quoted_tweets.unpersist() 
    #q_a.unpersist() 
    #q_a2.unpersist() 
    #truncated_quote_tweets.unpersist() 
    #untrunc_quote_tweets.unpersist() 
    #df_quoted_concat.unpersist() 
    #
    #retweet_tweets.unpersist() 
    #truncated_retweet_tweets.unpersist() 
    #untrunc_retweet_tweets.unpersist() 
    unpersist_dataframes()
    print("unpersisted!!")
    #time.sleep(10)
    
    #print("deleting dataframe")
    #del org_tweet_stream, quoted_tweets, q_a, q_a1, q_a2
    #del truncated_quote_tweets, trunc_quote_tw, untrunc_quote_tweets, untrunc_quote_tw, temp_df, retweet_tweets, truncated_retweet_tweets
    #del trunc_retweet_tw, untrunc_retweet_tweets, untrunc_retweet_tw
    
    print('############################### Parsing Original tweets #############################')
    
    #Filter tweets without any retweet or quoted tweet:
    quoted_retweet_ids= retweets_ids+quoted_ids_list
    original_tweets= tweet_stream.filter(~tweet_stream.samp_id.isin(quoted_retweet_ids)).persist()
    print("tweet_stream partitions: {}".format(tweet_stream.rdd.getNumPartitions()))
    original_tweets_count= original_tweets.count()
    print('original_tweets count: {}'.format(original_tweets_count))
    #print(original_tweets.columns)
    
    ################ Sensitive original tweets filtering ###############
    sensitive_ids= original_tweets.select('samp_id', 'samp_possibly_sensitive')
    sensitive_ids =sensitive_ids.rdd.map(lambda x: filter_func(x[0],x[1])).filter(lambda x: x is not None).collect()
    print('Sensitive tweet counts: {}'.format(len(sensitive_ids)))
    
    original_tweets= original_tweets.filter(~original_tweets.samp_id.isin(sensitive_ids)).persist()
    original_tweets_count= original_tweets.count()
    print('Original tweets after sensitive tweets filtered: {}'.format(original_tweets_count))
    
    #drop duplicates: Sanity check
    #original_tweets= original_tweets.sort(F.col("samp_id").asc(),F.col("samp_datetime").desc())
    #original_tweets= original_tweets.drop_duplicates(['samp_id']).persist()
    #original_tweets_count= original_tweets.count()
    #print('original_tweets after duplicates removed: {}'.format(original_tweets_count))
    #print('Partions for original_tweets: ', original_tweets.rdd.getNumPartitions())
    #original_tweets=original_tweets.repartition(8)
    #print('Partions for original_tweets: ', original_tweets.rdd.getNumPartitions())
    
    original_tweets= original_tweets.withColumn("org_id", original_tweets.samp_id).\
            withColumn('org_created_at',  original_tweets.samp_created_at).drop('samp_created_at')
    
    ############### Truncated Orginal tweets #######################
    truncated_org_tweets= original_tweets.filter(original_tweets.samp_truncated==True).persist() #pick extended tweet in that case
    truncated_org_tweets_count= truncated_org_tweets.count()
    print('truncated_org_tweets count: {}'.format(truncated_org_tweets_count))
    
    cols_to_keep= truncated_org_tweets.columns
    
    truncated_org_tweets= truncated_org_tweets.select([ F.col(c) if c in ['samp_id','samp_datetime', 'org_created_at' ,'org_id' ] \
                                else F.col(c).alias(c.replace('samp_', 'org_')) for c in cols_to_keep ])
    
    trunc_org_tw= truncated_org_tweets.select("org_id", "samp_id", "samp_datetime", "org_created_at", \
                F.col("org_extended_tweet.extended_entities.media.expanded_url").alias("org_url"),\
                F.col("org_extended_tweet.extended_entities.media.media_url").alias("org_media_url"), \
                F.col("org_extended_tweet.extended_entities.media.type").alias("org_media_type"), \
                F.col("org_extended_tweet.entities.urls.expanded_url").alias("org_attached_links"), \
                F.col("org_extended_tweet.full_text").alias('org_text'), \
                F.col("org_extended_tweet.entities.hashtags.text").alias("org_hashtags"), \
                F.col("org_extended_tweet.entities.user_mentions.screen_name").alias("org_user_metions"),\
                F.col("org_user.id").alias('user_id'), F.col("org_user.name").alias('user_name'), \
                F.col("org_user.screen_name").alias('user_screen_name'), F.col("org_user.verified").alias('user_verified'),
                F.col("org_user.profile_image_url").alias('user_profile_image_url'), \
                F.col("org_user.profile_banner_url").alias('user_profile_banner_url'), \
                F.col("org_user.profile_background_image_url").alias('user_profile_background_image_url'),
                F.col("org_user.followers_count").alias('user_followers_count'),\
                F.col("org_user.friends_count").alias('user_friends_count'), 'org_favorite_count', \
                'org_quote_count', 'org_reply_count', 'org_retweet_count')
    trunc_org_tw= trunc_org_tw.withColumn('org_url', convertUDF(F.col('org_url')))
    
    ############### Untruncated Original tweets #######################
    untrunc_org_tweets= original_tweets.filter(original_tweets.samp_truncated==False).persist() #pick extended tweet in that case
    untrunc_org_tweets_count= untrunc_org_tweets.count()
    print('untrunc_org_tweets count: {}'.format(untrunc_org_tweets_count ))
    
    cols_to_keep= untrunc_org_tweets.columns
    untrunc_org_tweets= untrunc_org_tweets.select([ F.col(c) if c in ['samp_id','samp_datetime', 'org_created_at' ,'org_id' ] \
                        else F.col(c).alias(c.replace('samp_', 'org_')) for c in cols_to_keep ])
    
    untrunc_org_tw= untrunc_org_tweets.select("org_id", "samp_id", "samp_datetime", "org_created_at",\
                F.col("org_extended_entities.media.expanded_url").alias("org_url"),\
                F.col("org_extended_entities.media.media_url").alias("org_media_url"), \
                F.col("org_extended_entities.media.type").alias("org_media_type"), \
                F.col("org_entities.urls.expanded_url").alias("org_attached_links"), "org_text",\
                F.col("org_entities.hashtags.text").alias("org_hashtags"), \
                F.col("org_entities.user_mentions.screen_name").alias("org_user_metions"),\
                 F.col("org_user.id").alias('user_id'), F.col("org_user.name").alias('user_name'), \
                F.col("org_user.screen_name").alias('user_screen_name'), F.col("org_user.verified").alias('user_verified'),
                F.col("org_user.profile_image_url").alias('user_profile_image_url'), \
                F.col("org_user.profile_banner_url").alias('user_profile_banner_url'), \
                F.col("org_user.profile_background_image_url").alias('user_profile_background_image_url'),
                F.col("org_user.followers_count").alias('user_followers_count'),\
                F.col("org_user.friends_count").alias('user_friends_count'), 'org_favorite_count', \
                'org_quote_count', 'org_reply_count', 'org_retweet_count')
    untrunc_org_tw= untrunc_org_tw.withColumn('org_url', convertUDF(F.col('org_url')))
    
    #Merge both truncated and untruncated and check for duplicate org_id:
    df_org_concat = trunc_org_tw.union(untrunc_org_tw).persist()
    #df_org_concat_count= df_org_concat.count()
    df_org_concat= df_org_concat.repartition(half_part).persist()
    #assert df_org_concat_count==original_tweets_count
    
    #print("will unperisist before merge")
    #time.sleep(10)
    #unpersist_dataframes()
    #time.sleep(10)
    ################# Merge original, quoted and retweet ##################
    print('############################### Merging tweets #############################')
    
    #time.sleep(300)
    print("df_quoted_concat partition: {}".format(df_quoted_concat.rdd.getNumPartitions()))
    print("df_retweet_concat partition: {}".format(df_retweet_concat.rdd.getNumPartitions()))
    
    retweet_quote_concat = df_quoted_concat.union(df_retweet_concat).repartition(repartition_num)
    all_tweets= retweet_quote_concat.union(df_org_concat).persist() #.repartition(repartition_num).persist()
    
    #Convert samp_timestamp: convert org_created_at at tne end of union and join:
    
    all_tweets= all_tweets.withColumn('org_created_at', F.regexp_extract(F.col("org_created_at"), "(\ \w+.+)", 1))
    all_tweets= all_tweets.withColumn('org_created_at', F.ltrim(all_tweets.org_created_at))
    all_tweets= all_tweets.withColumn('org_created_at', F.regexp_replace(F.col("org_created_at"), "(\+\d+\ )", ""))
                           
    all_tweets= all_tweets.withColumn('org_datetime', to_timestamp('org_created_at', 'MMM dd HH:mm:ss yyyy')).drop('org_created_at')
    
    dup_ids_count= all_tweets.groupby(['org_id']).count().where('count > 1').select('org_id').count()
    print('Duplicates after merging: {}'.format( dup_ids_count))
    
    #Removing duplicates after merging :
    print("all_tweets partitions: {}".format(all_tweets.rdd.getNumPartitions()))
    #all_tweets= all_tweets.repartition(repartition_num)
    #time.sleep(30)
    all_tweets= all_tweets.sort(F.col("org_id").asc(), F.col("samp_datetime").desc())  #use shuffle --> repartition
    print("all_tweets partitions: {}".format(all_tweets.rdd.getNumPartitions()))
    all_tweets= all_tweets.drop_duplicates(['org_id']).persist()
    all_tweets_unique_count= all_tweets.count()
    print('all_tweets_unique_count : {}'.format(all_tweets_unique_count))
    
    assert 0==all_tweets.groupby(['org_id']).count().where('count > 1').select('org_id').count()

    return org_tweet_stream_count, all_tweets_unique_count, all_tweets
    #return all_tweets

def filter_hiring_tweets(all_tweets):
    all_tweets= all_tweets.withColumn("org_text_hiring_tweets", all_tweets["org_text"]).select("*", F.lower(F.col('org_text_hiring_tweets')))\
                            .withColumn("lower(org_text_hiring_tweets)", F.regexp_replace(F.col("lower(org_text_hiring_tweets)"), "[’]", "'"))
    all_tweets2= all_tweets.filter(all_tweets['lower(org_text_hiring_tweets)'].rlike("|".join(hiring_terms))).drop('lower(org_text_hiring_tweets)', 'org_text_hiring_tweets').persist()
    print("Filtered tweets final count: {}".format( all_tweets2.count()))
    #print("all_tweets2 partitions: ", all_tweets2.rdd.getNumPartitions())
    return all_tweets2

        
def writing_tweets(tweets_df, destpath):
    #Writing parquet file
    un_identifier= datetime.now().strftime("%m%d%Y-%H%M%S")
    tweets_df.coalesce(1).write.parquet(destpath+'all_tweets_'+un_identifier+"_parquet")
    
    """ Converting function to UDF """
    convert_Arr_S_UDF = F.udf(lambda z: convert_array_tostring(z))
    
    df= tweets_df.withColumn("org_id",F.col("org_id").cast(StringType())).\
                    withColumn("samp_id",F.col("samp_id").cast(StringType())).\
                    withColumn("user_id",F.col("user_id").cast(StringType())).\
                   withColumn("org_media_url", convert_Arr_S_UDF(F.col('org_media_url'))).\
                   withColumn("org_attached_links", convert_Arr_S_UDF(F.col('org_attached_links'))).\
                   withColumn("org_media_type", convert_Arr_S_UDF(F.col('org_media_type'))).\
                    withColumn("org_hashtags", convert_Arr_S_UDF(F.col('org_hashtags'))).\
                    withColumn("org_user_metions", convert_Arr_S_UDF(F.col('org_user_metions')))
    
    # Replacing \n characters with |
    df= df.withColumn("org_text", F.regexp_replace(F.col("org_text"), "[\n]", "|"))
    df= df.withColumn("org_text", F.regexp_replace(F.col("org_text"), "[\"]", "\'"))
    
    df.coalesce(1).write.format('csv').option('header', 'true').option('path', destpath+'all_tweets_'+un_identifier+"_csv").save()
    
    return un_identifier

#replace folder with file: parquet

def removing_folder(source_path, un_identifier):

    print("------Removing Parquet folder-------")
    full_source_path= source_path+'all_tweets_'+un_identifier+'_parquet/'
    
    file_list= os.listdir(full_source_path)
    if os.path.exists(full_source_path):
        for f in file_list:
            if f.endswith('.parquet'):
                try:
                    #print(source_path+'all_tweets_'+un_identifier+'.parquet')
                    shutil.copyfile(full_source_path+f, source_path+'all_tweets_'+un_identifier+'.parquet')
                    print("File copied successfully.")
                
                # If source and destination are same
                except shutil.SameFileError:
                    print("Source and destination represents the same file.")
                     
                # If destination is a directory.
                except IsADirectoryError:
                    print("Destination is a directory.")
                     
                # If there is any permission issue
                except PermissionError:
                    print("Permission denied.")
                     
                # For other errors
                except e:
                    print("Error occurred while copying file.", e)
                    
    try:
        shutil.rmtree(full_source_path)
        print("Folder removed successfully.")
    except e:
        print("Error: %s : %s" % (full_source_path, e.strerror))
        
    print("------Removing CSV folder-------")
    full_source_path= source_path+'all_tweets_'+un_identifier+'_csv/'

    print('full_source_path: {}'.format(full_source_path))
    if os.path.exists(full_source_path):
        file_list= os.listdir(full_source_path)
        
        for f in file_list:
            if f.endswith('.csv'):
                try:
                    shutil.copyfile(full_source_path+f, source_path+'all_tweets_'+un_identifier+'.csv')
                    print("File copied successfully.")
     
                # If source and destination are same
                except shutil.SameFileError:
                    print("Source and destination represents the same file.")
                     
                # If destination is a directory.
                except IsADirectoryError:
                    print("Destination is a directory.")
                     
                # If there is any permission issue
                except PermissionError:
                    print("Permission denied.")
                     
                # For other errors
                except e:
                    print("Error occurred while copying file.", e)
                    
    
    try:
        shutil.rmtree(full_source_path)
        print("Folder removed successfully.")
    except e:
        print("Error: %s : %s" % (full_source_path, e.strerror))

#########################
hiring_terms= ["REQUEST RECRUITMENT FORM", "is[\S\s]+hiring", "are[\S\s]+hiring", "is[\S\s]+recruiting", "are[\S\s]+recruiting", \
               "application form", "now hiring", "hiring now", "now recruiting", "recruiting now", "\'re[\S\s]+hiring",\
               "\'re[\S\s]+recruiting", "\'re[\S\s]+interested", "recruitment form", "open positions",\
                "interested .+candidates", "apply now", "to apply", "to join", "are looking for", "\'re looking for", \
               "to register", "join us","send .+resume", "will .+hiring", "will .+recruiting", "still hiring", "are .+open",\
              "hiring for", "recruiting for"]
file_unprocessed_count={}
file_processed_count= {}
order_file_list
run_ordered_file_list= order_file_list() #ordered_file_list[:]
run_ordered_file_len= len(run_ordered_file_list)

try: 
    #spark = SparkSession.builder.master("local[repartition_num]").appName('Twitter_preprocess').getOrCreate()
    print('Shuffle partition: {}'.format(spark.conf.get('spark.sql.shuffle.partitions')))
    spark.conf.set('spark.sql.shuffle.partitions', half_part)
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    print('Shuffle partition: {}'.format( spark.conf.get('spark.sql.shuffle.partitions')))
    print('SparkUI available at: {}'.format( spark.sparkContext.uiWebUrl))
    
    ctr=1
    for f in run_ordered_file_list:
        print("-----------------------Processing {} of {}: {}----------------------------------".format(ctr,run_ordered_file_len,f))
        file_mem= np.round(os.path.getsize(input_path+f)/(1024*1024), 3)
        print("file memory: {}".format(file_mem))
        start = time.time()
        rawcnt, actual_cnt, all_tweets= processing_file(input_path+f, spark, repartition_num)
        #all_tweets= processing_file(input_path+f, spark, repartition_num)
        all_tweets_filtered= filter_hiring_tweets(all_tweets)
        end = time.time()
        elapsedtime= end-start
        print("Time took to process: {}MB file size is : {} seconds".format(file_mem, elapsedtime))
        un_identifier= writing_tweets(all_tweets_filtered, destpath)
        removing_folder(source_path, un_identifier)
        unpersist_dataframes()
        file_unprocessed_count[f]= rawcnt
        file_processed_count[f]= actual_cnt
        ctr+=1
    #time.sleep(500)

except Exception as e:
    print("Exception occured at: {}".format(e))
    if spark.getActiveSession():
        spark.stop()
    print("Spark_Session stopped")
    #print('Checkpoint at {} and file: {}'.format(ctr-1, run_ordered_file_list[ctr-1]))

finally:
    print('Total record count: {}'.format(sum(file_processed_count.values())))
    # write count of file
    with open (LOG_DIR+'/count_details'+time_stmp+'.txt', 'w') as convert_file:
        convert_file.write('------------------ Unprocessed count ------------ \n\n')
        convert_file.write(str(file_unprocessed_count))
        convert_file.write(' \n\n ------------------ Processed count ------------ \n\n ')
        convert_file.write(str(file_processed_count))