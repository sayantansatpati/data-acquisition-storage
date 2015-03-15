#!/usr/bin/env python

__author__ = 'ssatpati'


from util.config import Config
from util.mongo import Mongo
import collections
import itertools
import tweepy
import time
import os
from util import log
from nltk import *

logger = log.get_logger(__name__)

class Analyze(object):

    DB_STREAM = "db_streamT"
    DB_TWEETS = "db_tweets"
    DB_FOLLOWERS = "db_followers"

    MAX_RATE_LIMIT_COUNT = 5
    SLEEP_MINS = 15

    USERS_FILE = "output_users.txt"

    def __init__(self):
        self.config = Config()

    def top_30_retweets(self):
        """Top 30 retweets from tweets in db_tweets"""

        # Store tweets in an Ordered Dictionary so that it can be sorted
        od = collections.OrderedDict()

        db_stream = Mongo(self.DB_STREAM)
        db_tweets = Mongo(self.DB_TWEETS)

        stream_tweets = db_stream.collection()
        tweets = db_tweets.collection()

        # For each tweet in db_tweets, find matching Tweet in db_streamT
        for c1 in tweets.find({}, {'tweet': 1}):
            t = c1["tweet"].strip()
            for c2 in stream_tweets.find({'text': t}, {'retweet_count': 1, 'id': 1, 'user.screen_name': 1, 'user.location': 1}):
                od[t] = (c2, int(c2["retweet_count"]))
                #logger.info(c2)

        # Sort Ordered Dictionary by retweet_count DESC
        od = collections.OrderedDict(sorted(od.items(), key=lambda t: t[1][1], reverse=True))

        # Print Top 30 Re-tweets (And write to a file for later processing)
        if os.path.exists(self.USERS_FILE):
            os.remove(self.USERS_FILE)

        with open(self.USERS_FILE, "a") as f:
            for k, v in itertools.islice(od.items(), 0, 30):
                logger.info(v)
                f.write(str(v[1]) +
                        "," +
                        str(v[0]["id"]) +
                        "," +
                        v[0]["user"]["screen_name"].encode('utf-8') +
                        "," +
                        k.encode('utf-8')
                        + "\n")

    def lexical_diversity_tweets(self):
        """Compute Lexical Diversity & Store it Back in Collection"""
        db_stream = Mongo(self.DB_STREAM)
        stream_tweets = db_stream.collection()

        ld_list = []

        for c in stream_tweets.find({}, {'text': 1}):
            ld = self.__lexical_diversity(c["text"])
            ld_list.append(ld)
            #logger.info(c["_id"], str(ld), c["text"])
            logger.info(c)
            # Update Lexical Diversity
            ld_dict = {"lexical_diversity": ld}
            stream_tweets.update({'_id': c["_id"]}, {"$set": ld_dict}, upsert=False)

        # Plot Lexical Diversity
        fdist = FreqDist(ld_list)
        logger.info(fdist.items())
        fdist.plot()

    def followers(self):
        """Find followers"""
        db_followers = Mongo(self.DB_FOLLOWERS)
        followers = db_followers.collection("followers")

        api = self.__get_tweepy_api()

        rate_limit_cnt = 0

        # Read File with Screen Names
        with open(self.USERS_FILE, "r") as f:
            lines = f.readlines()
            cnt = 0
            # For each Screen Name
            while cnt < len(lines):
                l = lines[cnt]
                follower_list = [] # Per User
                user_id = l.split(",")[1]
                screen_name = l.split(",")[2]
                logger.info("[%d] Getting Followers for screen name: %s", cnt, screen_name)
                try:
                    for page in tweepy.Cursor(api.followers_ids, screen_name=screen_name).pages():
                        follower_list.extend(page)
                        time.sleep(60) # Uncomment this if throwing rate limit error
                except tweepy.TweepError as te:
                    # Check whether Max Rate Limiting has been reached
                    rate_limit_cnt += 1
                    if rate_limit_cnt >= self.MAX_RATE_LIMIT_COUNT:
                        raise
                    logger.error("[User: %s] Exception while getting followers: %s", screen_name, te)
                    logger.info("[%d] Sleeping for %d mins", rate_limit_cnt, self.SLEEP_MINS)
                    time.sleep(60 * self.SLEEP_MINS)
                    continue  # Without incrementing count

                logger.info("Total # of followers: %d", len(follower_list))
                followers.insert({"id": user_id, "screen_name": screen_name, "num_followers": len(follower_list), "followers": follower_list})
                cnt += 1 # Process next user if everything is successful

    def unfollowers(self):
        """Unfollowers ever since data was collected"""
        db_followers = Mongo(self.DB_FOLLOWERS)
        followers = db_followers.collection("followers")
        # Get users with highest number of followers
        cnt = 0
        last_user = None
        for c in followers.find({}, {'id': 1, 'screen_name': 1, 'num_followers': 1, 'followers': 1}).sort('num_followers', -1):
            if last_user is None:
                last_user = c['screen_name']
            else:
                if last_user == c['screen_name']:
                    last_user = c['screen_name']
                    continue


            logger.info("{0} : {1}".format(c['screen_name'], c['num_followers']))

            # Fetch new followers using tweepy
            followers = []

            api = self.__get_tweepy_api()
            rate_limit_cnt = 0
            while rate_limit_cnt <= self.MAX_RATE_LIMIT_COUNT:
                try:
                    for page in tweepy.Cursor(api.followers_ids, screen_name=c['screen_name']).pages():
                        followers.extend(page)
                        time.sleep(60) # Uncomment this if throwing rate limit error
                    break
                except tweepy.TweepError as te:
                    # Check whether Max Rate Limiting has been reached
                    rate_limit_cnt += 1
                    if rate_limit_cnt >= self.MAX_RATE_LIMIT_COUNT:
                        raise
                    logger.error("[User: %s] Exception while getting followers: %s", screen_name, te)
                    logger.info("[%d] Sleeping for %d mins", rate_limit_cnt, self.SLEEP_MINS)
                    time.sleep(60 * self.SLEEP_MINS)
                    continue  # Without incrementing count

            if len(followers) != len(c['followers']):
                old_followers = set(c['followers'])
                new_followers = set(followers)

                logger.info("Lengths of old:{0} & new:{1} followers don't match".format(len(old_followers), len(new_followers)))
                logger.info("Unfollowed Friends of {0}: {1}".format(c['screen_name'], old_followers - new_followers))

            last_user = c['screen_name']
            cnt += 1
            if cnt >= 10:
                break


    def __lexical_diversity(self, text):
        l = text.split()
        return float(len(set(l))) / len(l)


    def __get_tweepy_api(self):
        """Tweepy API"""
        auth = tweepy.OAuthHandler(self.__get_config_val("consumer_key"), self.__get_config_val("consumer_secret"))
        auth.set_access_token(self.__get_config_val("access_token"), self.__get_config_val("access_token_secret"))
        return tweepy.API(auth)
        pass

    def __get_config_val(self, key):
        return self.config.get("twitter", key)