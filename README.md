# bigdata
 Real-Time Twitter Data Analysis using Spark Streaming
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.twitter import TwitterUtils
import matplotlib.pyplot as plt
from textblob import TextBlob

# Twitter API credentials (replace with your own credentials)
consumer_key = "your_consumer_key"
consumer_secret = "your_consumer_secret"
access_token = "your_access_token"
access_token_secret = "your_access_token_secret"

# Set up Twitter authentication
conf = SparkConf().setAppName("TwitterStreamAnalysis")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)  # 10-second window

# Connect to Twitter Stream
tweets = TwitterUtils.createStream(ssc, consumer_key, consumer_secret, access_token, access_token_secret)

# Process tweets and perform sentiment analysis
def analyze_sentiment(tweet):
    analysis = TextBlob(tweet)
    return analysis.sentiment.polarity

# Extract tweet text and apply sentiment analysis
tweet_texts = tweets.map(lambda tweet: tweet.getText())
sentiment_scores = tweet_texts.map(analyze_sentiment)

# Print sentiment scores
sentiment_scores.pprint()

# Visualize sentiment scores
def visualize_sentiments(rdd):
    if not rdd.isEmpty():
        scores = rdd.collect()
        plt.hist(scores, bins=50, alpha=0.7)
        plt.title('Sentiment Distribution')
        plt.xlabel('Sentiment')
        plt.ylabel('Frequency')
        plt.show()

sentiment_scores.foreachRDD(visualize_sentiments)

# Start the streaming context
ssc.start()
ssc.awaitTermination()
