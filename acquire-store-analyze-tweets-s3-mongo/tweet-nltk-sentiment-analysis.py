#!/usr/bin/env python

__author__ = 'ssatpati'

import random
import nltk
import pprint

USERS_FILE = "users.txt"

pos_tweets = [('I love this car', 'positive'),
              ('This view is amazing', 'positive'),
              ('I feel great this morning', 'positive'),
              ('I am so excited about the concert', 'positive'),
              ('He is my best friend', 'positive')]


neg_tweets = [('I do not like this car', 'negative'),
              ('This view is horrible', 'negative'),
              ('I feel tired this morning', 'negative'),
              ('I am not looking forward to the concert', 'negative'),
              ('He is my enemy', 'negative')]

word_features = None


def get_words_in_tweets(tweets):
    all_words = []
    for (words, sentiment) in tweets:
        all_words.extend(words)
    return all_words


def get_word_features(wordlist):
    wordlist = nltk.FreqDist(wordlist)
    pprint.pprint(wordlist)
    return wordlist.keys()


def extract_features(document):
    document_words = set(document)
    features = {}
    for word in word_features:
        features['contains(%s)' % word] = (word in document_words)
    return features


def sentiment_analysis():
    tweets = []
    for (words, sentiment) in pos_tweets + neg_tweets:
        words_filtered = [e.lower() for e in words.split() if len(e) >= 3]
        tweets.append((words_filtered, sentiment))

    random.shuffle(tweets)
    print("@@@ All Tweets after shuffle")
    pprint.pprint(tweets)

    '''
    print("@@@ Random Sample of 5 tweets")
    test_tweets = random.sample(tweets, 5)
    pprint.pprint(test_tweets)
    '''

    global word_features
    word_features = get_word_features(get_words_in_tweets(tweets))
    print("@@@ Word Features")
    print(word_features)

    training_set = nltk.classify.apply_features(extract_features, tweets)
    print("@@@ Training Set")
    pprint.pprint(training_set)

    print("@@@ Training Classifier using Training Set")
    classifier = nltk.NaiveBayesClassifier.train(training_set)

    with open(USERS_FILE, "r") as f:
        lines = f.readlines()
        for l in lines:
            tweet_text = l.strip().split(",")[3]
            print(tweet_text)
            print(classifier.classify(extract_features(tweet_text)))
            print("-----------------------------------")

if __name__ == '__main__':
    sentiment_analysis()