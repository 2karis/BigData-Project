from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import datetime
from nltk.corpus import stopwords
from nltk.sentiment import vader
from nltk.corpus import words

import string


printable = set(string.printable)
stop_words = stopwords.words('english')
english_words = words.words()
analyzer = vader.SentimentIntensityAnalyzer()


def analyse(sentence):
    #for sentence in sentences:
    vs = analyzer.polarity_scores(sentence)
    compound = float(vs['compound'])
    score = ""
    #max_val =max([float(vs['neg']),float(vs['pos']),float(vs['neu'])])
    if compound >= 0.05:
        score = "positive"
    if compound > -0.05 and compound < 0.05:
        score = "neutral"
    if compound <= -0.05:
        score = "negative"


    return (sentence,float(vs['compound']), score)

def remove(sentence):
    word_list = str(sentence).split(" ")
    ret_word =""
    for word in word_list:
        #if word not in stop_words and word.isalpha() and word != "RT":
        if word.isalpha() and word != "RT" and word in english_words:
            ret_word += word + " "
    return ret_word

def output(x):
    print(x)

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def process(rdd):

    print("========= %s =========" % str(datetime.datetime.now()))
    rdd.collect()
    print(rdd.count)
    if 1 > 0:
        spark = getSparkSessionInstance(rdd.context.getConf())

        rowRdd = rdd.map(lambda w: Row(tweet=w[0],rating=w[1], score=w[2]))
        wordsDataFrame = spark.createDataFrame(rowRdd)
        wordsDataFrame.createOrReplaceTempView("tweets")

        wordScoreDataFrame = \
            spark.sql("select tweet, rating, score  from tweets")
        wordScoreDataFrame.show()

        wordCountDataFrame = \
            spark.sql("select score ,count(score)  from tweets group by score")
        wordCountDataFrame.show()


def empty_rdd():
    print("###The current RDD is empty. Wait for the next complete RDD ###")


sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 9009)

tweet = lines.flatMap(lambda line: line.split("b'"))\
    .map(lambda word: str(word[:-1]))\
    .map(lambda word: remove(word))\
    .map(lambda word: analyse(word))


tweet.pprint()

# Print the first ten elements of each RDD generated in this DStream to the console
#wordCounts.pprint()
tweet.foreachRDD(lambda rdd: empty_rdd() if rdd.count() == 0 else process(rdd))
#print(words)
#words.saveAsTextFiles('dstream')

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminatev