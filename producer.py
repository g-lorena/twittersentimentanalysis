import tweepy
from kafka import KafkaProducer
import logging

"""API ACCESS KEYS"""

consumerKey = "XXXX"
consumerSecret = "XXXX"
accessToken = "XXX-XXX"
accessTokenSecret = "XXXX"

producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'Bitcoin'
topic_name = 'twitter'


def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api


class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    twitter_stream = TweetListener(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    twitter_stream.start_streaming_tweets(search_term)
    # create the streaming object
    #api = twitterAuth() ['#python']
    #stream = MyTwitterStream(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    #stream.start_streaming_tweets(search_term)

