#!/usr/bin/env python

__author__ = 'ssatpati'

import ConfigParser


class Config(object):

    def __init__(self):
        self.config = ConfigParser.ConfigParser()
        self.config.read("/Users/ssatpati/0-DATASCIENCE/DEV/github/data-acquisition-storage/acquire-store-analyze-tweets-s3-mongo/util/config.ini")

    def get(self, section, option, raw=False, vars=None):
        return self.config.get(section, option, raw, vars)
