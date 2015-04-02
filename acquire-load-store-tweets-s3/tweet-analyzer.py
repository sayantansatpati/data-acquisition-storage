__author__ = 'ssatpati'

import tweepy
import datetime
import time
import os
import sys
import glob
import codecs
import shutil
import signal
from nltk import *
from nltk.corpus import stopwords
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto

'''Twitter Query'''
QUERY = "microsoft AND mojang"
SINCE = "2015-03-25T00:00:00"
UNTIL = "2015-04-02T00:00:00"
LIMIT_TWEETS = None
PARTITION_HOURS = 24

'''Date & Time Formats used to partition the Data into Files'''
DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
FILE_DATE_TIME_FORMAT = "%Y-%m-%d_%H-%M-%S"

'''Output DIR'''
OUTPUT_DIR = "output"

'''S3 Bucket'''
BUCKET_NAME="assignment2-tweets"

'''RATE LIMITING'''
MAX_RATE_LIMIT_COUNT = 5
SLEEP_MINS = 15


def signal_handler(signal, frame):
        print('You pressed Ctrl+C, Aborting!!!')
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def acquire_store_analyze_tweets():
    """Acquire, Load, & Store Tweets"""
    count = 0
    tokens = []

    # Acquire Tweets & Store in Local Files
    acquire_store_tweets()

    # Move tweets to AWS S3 Bucket
    move_tweets_s3(BUCKET_NAME)

    # Load Tweets from Local
    os.environ["NLTK_DATA"] = "/Users/ssatpati/nltk_data"
    sw = stopwords.words('english')
    tokenizer = RegexpTokenizer(r'\w+')
    for tweet in load_tweets():
        l = [t for t in tokenizer.tokenize(tweet) if t.lower() not in sw]
        tokens.extend(l)
        count += 1

    print(tokens)
    print("[INFO] Total Number of Tweet File Processed: {0}; Total Number of Tokens: {1}".format(count, len(tokens)))

    fdist = FreqDist(tokens)
    print(fdist.items())
    fdist.tabulate(20)  # Show Top 20
    fdist.plot()

    # Clean Up Local Files


def load_tweets():
    """Load Tweets from Local Files"""
    for root, dirnames, filenames in os.walk(OUTPUT_DIR):
        for filename in filenames:
            if filename.startswith("tweet"):
                with codecs.open(OUTPUT_DIR + "/" + filename, 'r', encoding='utf8') as f:
                    yield f.read()


def acquire_store_tweets(query=QUERY, since=SINCE, until=UNTIL, partition_hours=PARTITION_HOURS, limit_tweets=None):
    """Acquire Files using Tweepy API"""
    api = get_tweepy_api()

    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR)
    else:
        os.makedirs(OUTPUT_DIR)

    # Convert into DateTime
    since_dt = datetime.datetime.strptime(since, DATE_TIME_FORMAT)
    until_dt = datetime.datetime.strptime(until, DATE_TIME_FORMAT)

    # Counters
    cnt_across = 0
    rate_limit_cnt = 0

    # Tweets are acquired & stored for every Date Partition, which is configurable
    for t1, t2 in date_partition(since_dt, until_dt, partition_hours):
        cnt = 0
        # Tweets
        t_f_name = "".join(["./",
                          OUTPUT_DIR, "/tweet",
                          "_",
                          t1.strftime(FILE_DATE_TIME_FORMAT),
                          "__",
                          t2.strftime(FILE_DATE_TIME_FORMAT)])
        # Raw Tweets
        rt_f_name = "".join(["./",
                          OUTPUT_DIR, "/raw_tweet",
                          "_",
                          t1.strftime(FILE_DATE_TIME_FORMAT),
                          "__",
                          t2.strftime(FILE_DATE_TIME_FORMAT)])

        ft = open(t_f_name, "a")
        frt = codecs.open(rt_f_name, "a", "utf-8")
        print("Tweets Output File:{0}".format(t_f_name))
        print("Raw Tweets Output File:{0}".format(rt_f_name))

        while True:
            try:
                for tweet in tweepy.Cursor(api.search,
                                           q=query,
                                           since=t1.strftime(DATE_TIME_FORMAT),
                                           until=t2.strftime(DATE_TIME_FORMAT)).items(limit_tweets):
                    #print(tweet)
                    frt.write(str(tweet))
                    frt.write("\n")
                    ft.write(tweet.text.encode('utf-8') + "\n")
                    cnt += 1
                    cnt_across += 1
            except tweepy.TweepError as te:
                print(te)
                rate_limit_cnt += 1

                # Exit if max Rate Limit is reached
                if rate_limit_cnt >= MAX_RATE_LIMIT_COUNT:
                    raise
                else:  # Sleep and continue
                    time.sleep(60 * SLEEP_MINS)
                    continue
            # Break, if everything goes well
            break

        print("~ {0} Tweets Dumped from {1} TO {2}\n".format(cnt, t1, t2))
        ft.close()
        frt.close()
    print "Total Number of Tweets Acquired: {0}".format(cnt_across)


def date_partition(start, end, partition_hours):
    """Partition Start and End Date by Partition Hours"""
    return datetime_partition(start, end, datetime.timedelta(hours=partition_hours))


def datetime_partition(start, end, duration):
    """Date Time Partition Generator"""
    current = start
    while end > current:
        yield (current + datetime.timedelta(seconds=1), current + duration)
        current = current + duration


def get_tweepy_api():
    """Tweepy API"""
    consumer_key = "fRr3Rrd08IKUQTEuHzJcw"
    consumer_secret = "hyA4XrtmYOrxlw8dAn97Ji7RLMRWZfOdOaAfih3UdM"

    access_token = "2254156519-OfuXeTslD47o0YpR3vEBqXyIMu50GpPd7yecFtT"
    access_token_secret = "43DzymYK5LWimKewVWuOgqSnEz61fSDSk8fA7FomHLd1O"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    return tweepy.API(auth)


def move_tweets_s3(bucket_name):
    """Move Tweets to S3 Bucket"""
    conn = S3Connection('AKIAJ6T7UIAPPXKXVSEQ', 'XjBk/5K1CABO0fRwfHhEH+gF26yGwVYxWOwa/aTF')

    # Delete Bucket contents and Bucket, if one is available
    bucket = None
    try:
        bucket = conn.get_bucket(bucket_name)
        # Delete Files
        print("Deleting Files in S3 Bucket - {0}".format(bucket_name))
        for key in bucket.list():
            key.delete()
        print("Deleting S3 Bucket - {0}".format(bucket_name))
        # Delete Bucket
        conn.delete_bucket(bucket_name)
    except boto.exception.S3ResponseError as err:
        print(err)

    bucket = conn.create_bucket(bucket_name)

    print("Bucket Created in S3 - {0}".format(bucket_name))

    cnt = 0
    for name in glob.glob(OUTPUT_DIR + "/tweet_*"):
        print("Copying {0} to S3: {1}".format(name, bucket_name))
        cnt += 1
        k = Key(bucket)
        k.key = name.split("/")[-1]
        k.set_contents_from_filename(name)

    print("{0} Files Copied into S3 Bucket - {1}".format(cnt, bucket_name))


if __name__ == '__main__':
    '''Main Point of Entry to Program'''
    acquire_store_analyze_tweets()
