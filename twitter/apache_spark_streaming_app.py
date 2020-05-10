from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys


# on Windows:
# >venv\Scripts\python.exe twitter\apache_spark_streaming_app.py

# socket connection params
TCP_IP = "localhost"
TCP_PORT = 9009

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql(
            "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
    except ValueError:
        e = sys.exc_info()
        print("Error: %s" % e[0])
        print("%s" % e[1])
    except ConnectionResetError:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
    except:
        e = sys.exc_info()
        print("Error: %s" % e[0])
        print("%s" % e[1])


if __name__ == '__main__':
    # create spark configuration
    _conf = SparkConf()
    _conf.setAppName('TwitterHashtagStreamApp')

    # create spark context
    sc = SparkContext(conf=_conf)
    sc.setLogLevel('ERROR')

    # create streaming context
    ssc = StreamingContext(sc, 10)

    # set a checkpoint
    ssc.checkpoint("checkpoint_TwitterApp")

    # read data from port
    dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split(" "))

    # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)

    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()

    # wait for the streaming to finish
    ssc.awaitTermination()
