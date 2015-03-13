#!/usr/bin/env python

__author__ = 'ssatpati'

from store import Store
from analyze import Analyze
import signal
import sys


def signal_handler(signal, frame):
        print('You pressed Ctrl+C, Aborting!!!')
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
#print('Press Ctrl+C')
#signal.pause()

if __name__ == '__main__':
    '''Main Point of Entry to Program'''

    s = Store()
    # Populate Tweets (Storing Task)
    #s.store_tweets()
    #s.move_tweets_s3_mongo()

    # Retrieve and Analyze Tweets
    a = Analyze()
    #a.top_30_retweets()
    #a.lexical_diversity_tweets()
    a.followers()

