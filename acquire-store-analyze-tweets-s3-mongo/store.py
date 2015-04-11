#!/usr/bin/env python

__author__ = 'ssatpati'

import shutil
import os
import glob

import tweepy
from boto.s3.connection import S3Connection
import boto

from util.config import Config
from util.mongo import Mongo
from util import log


logger = log.get_logger(__name__)

TWEETS_DIR = "output_tweets"

class Store(object):

    QUERY = "microsoft AND mojang"
    SINCE = "2015-03-01T00:00:00"
    UNTIL = "2015-03-15T00:00:00"
    LIMIT_TWEETS = None
    PARTITION_HOURS = 24

    '''Date & Time Formats used to partition the Data into Files'''
    DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    FILE_DATE_TIME_FORMAT = "%Y-%m-%d_%H-%M-%S"

    '''Output DIR'''
    OUTPUT_DIR = "output"

    def __init__(self):
        self.config = Config()

    def fetch_tweets(self, query=QUERY, since=SINCE, until=UNTIL, limit_tweets=None):
        """Acquire Files using Tweepy API"""
        api = self.__get_tweepy_api()
        cnt = 0

        for tweet in tweepy.Cursor(api.search,
                                q=query,
                                since=since,
                                until=until).items(limit_tweets):
                #print(tweet)
                #tweet.text.encode('utf-8')
                yield tweet
                cnt += 1

        if cnt == 0:
            raise Exception("No Tweets Acquired between {0} and {1}".format(since, until))
        logger.info("# Total Tweets Acquired between {0} and {1}: {2}".format(since, until, cnt))

    def store_tweets(self, db_name="db_streamT"):
        """Store Tweets into MongoDB db_streamT, collection: tweets"""
        mongodb = Mongo(db_name)
        batch_size = int(mongodb.get_batch_size())
        batch_tweets = []
        for tweet in self.fetch_tweets():
            print tweet._json
            batch_tweets.append(tweet._json)

            if len(batch_tweets) == batch_size:
                logger.info("Inserting Batch into Mongo")
                mongodb.insert(batch_tweets)
                batch_tweets = [] #Clear

        logger.info("Inserting Remaining from Batch into Mongo")
        mongodb.insert(batch_tweets)


    def move_tweets_s3_mongo(self, db_name="db_tweets"):
        """Fetch Checked Tweets from S3 and store them in MongoDB db_tweets, collection tweets"""
        if os.path.exists(TWEETS_DIR):
            shutil.rmtree(TWEETS_DIR)
            os.makedirs(TWEETS_DIR)
        else:
            os.makedirs(TWEETS_DIR)

        mongodb = Mongo(db_name)
        batch_size = int(mongodb.get_batch_size())
        batch_chucked_tweets = []

        conn = S3Connection(self.config.get("aws", "access_key_id"),
                            self.config.get("aws", "secret_access_key"))

        try:
            bucket = conn.get_bucket(self.config.get("aws", "bucket_name"))
            for key in bucket.list():
                key.get_contents_to_filename("{0}/{1}".format(TWEETS_DIR, key.name))
        except boto.exception.S3ResponseError as err:
            print(err)
            raise

        cnt = 0;

        for name in glob.glob(TWEETS_DIR + '/*'):
            print(name)
            with open(name, "r") as f:
                lines = f.readlines()
                for line in lines:
                    cnt += 1
                    t = {"tweet": line}
                    batch_chucked_tweets.append(t)

                    if len(batch_chucked_tweets) == batch_size:
                        logger.info("Inserting Batch into Mongo")
                        mongodb.insert(batch_chucked_tweets)
                        batch_chucked_tweets = [] #Clear

        logger.info("Inserting Remaining from Batch into Mongo")
        mongodb.insert(batch_chucked_tweets)
        logger.info("# Total Chucked Tweets: {0}".format(cnt))


    def __get_tweepy_api(self):
        """Tweepy API"""
        auth = tweepy.OAuthHandler(self.__get_config_val("consumer_key"), self.__get_config_val("consumer_secret"))
        auth.set_access_token(self.__get_config_val("access_token"), self.__get_config_val("access_token_secret"))
        return tweepy.API(auth)
        pass


    def __get_config_val(self, key):
        return self.config.get("twitter", key)


