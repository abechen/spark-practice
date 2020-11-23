from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

_KAFKA_HOST = '127.0.0.1:9092'
_KAFKA_INPUT_TOPIC = 'bank-transaction'
_KAFKA_GROUP_ID = 'sp-filter'

def get_spark_session():
    conf = SparkConf() \
            .setAppName('{}-spark-streaming'.format(_KAFKA_INPUT_TOPIC)) \
            .set('spark.driver.maxResultSize', '0') \
            .set('spark.executor.core', '1') \
            .set('spark.executor.memory', '1g') \
            .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.elasticsearch:elasticsearch-hadoop:7.5.0')

    sc = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    sc.sparkContext.setLogLevel('WARN')

    return sc

def set_schema():
    schema = StructType() \
        .add('account_no', StringType()) \
        .add('date', StringType()) \
        .add('transaction_details', StringType()) \
        .add('value_date', StringType()) \
        .add('withdrawal_amt', IntegerType()) \
        .add('balance_amt', IntegerType()) \
        .add('event_time', TimestampType())

    return schema

if __name__ == '__main__':
    # [SPARK SESSION]
    print('Starting a spark session')
    sc = get_spark_session()
    
    print('Set kafka input schema')
    schema = set_schema()

    # [SOURCE]
    df_json = sc \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', _KAFKA_HOST) \
        .option('subscribe', _KAFKA_INPUT_TOPIC) \
        .load()
        #.option('kafka.group.id', _KAFKA_GROUP_ID) \
    # parse json value
    df = df_json \
        .selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)') \
        .select(from_json(col('value'), schema).alias('transaction_data')) \
        .select('transaction_data.*')

    # [SINK]
    # === elasticsearch ===
    qry = df.writeStream \
        .format('org.elasticsearch.spark.sql') \
        .outputMode('append') \
        .option('es.nodes.wan.only', 'true') \
        .option('es.nodes', 'localhost') \
        .option('es.port', 9200) \
        .option('es.index.auto.create','true') \
        .option('es.resource', 'bank-transaction') \
        .option('checkpointLocation', './tmp/checkpoint-{}'.format(_KAFKA_GROUP_ID)) \
        .trigger(processingTime='10 second') \
        .start()

    qry.awaitTermination()
