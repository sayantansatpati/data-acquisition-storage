#!/usr/bin/env python

import os
import subprocess
import datetime
from boto.s3.connection import S3Connection
import glob
import boto
from boto.s3.key import Key
from util.config import Config


__author__ = 'ssatpati'

BACKUP_RESTORE_DIR = "dump"
S3_BUCKET = "assignment-3-mongo-backup"

DB_STREAM = "db_streamT"
DB_TWEETS = "db_tweets"

BACKUP_RESTORE_DIR_DB_STREAM = BACKUP_RESTORE_DIR + "/" + DB_STREAM
BACKUP_RESTORE_DIR_DB_TWEETS = BACKUP_RESTORE_DIR + "/" + DB_TWEETS


def backup():
    dt = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Rename any previous dump
    if os.path.exists(BACKUP_RESTORE_DIR):
        os.rename(BACKUP_RESTORE_DIR, BACKUP_RESTORE_DIR + "_" + dt)

    cmd(["pwd"])
    cmd(['mongodump', '--db', DB_TWEETS])
    cmd(['mongodump', '--db', DB_STREAM])

    if not os.path.exists(BACKUP_RESTORE_DIR_DB_STREAM):
        raise Exception("Backup Not Created, Aborting!!!")

    if not os.path.exists(BACKUP_RESTORE_DIR_DB_TWEETS):
        raise Exception("Backup Not Created, Aborting!!!")

    # Put Files into S3
    put_s3()


def restore():
    dt = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Rename any previous dump
    if os.path.exists(BACKUP_RESTORE_DIR):
        os.rename(BACKUP_RESTORE_DIR, BACKUP_RESTORE_DIR + "_" + dt)

    get_s3()

    if not os.path.exists(BACKUP_RESTORE_DIR_DB_STREAM):
        raise Exception("Backup Not Created, Aborting!!!")

    if not os.path.exists(BACKUP_RESTORE_DIR_DB_TWEETS):
        raise Exception("Backup Not Created, Aborting!!!")

    cmd(["pwd"])
    cmd(['mongorestore', '--db', DB_TWEETS, '--drop', BACKUP_RESTORE_DIR_DB_TWEETS])
    cmd(['mongorestore', '--db', DB_STREAM, '--drop', BACKUP_RESTORE_DIR_DB_STREAM])


def put_s3():
    """Move Backup to S3 Bucket"""
    config = Config()
    conn = S3Connection(config.get("aws", "access_key_id"), config.get("aws", "secret_access_key"))

    # Delete previous backup if available
    bucket = None
    try:
        bucket = conn.get_bucket(S3_BUCKET)
        print("Bucket Available in S3 - {0}".format(S3_BUCKET))
        # Delete Files inside Bucket
        for key in bucket.list():
            print(key.name)
            if BACKUP_RESTORE_DIR in key.name:
                print("Deleting key: %s" % key.name)
                key.delete()
    except boto.exception.S3ResponseError as err:
        print(err)
        bucket = conn.create_bucket(S3_BUCKET)
        print("Bucket Created in S3 - {0}".format(S3_BUCKET))

    try:
        for f in glob.glob(BACKUP_RESTORE_DIR_DB_TWEETS + "/*"):
            k = Key(bucket)
            k.key = f
            k.set_contents_from_filename(f)
            print("Stored {0} in S3 Bucket {1}\n\n".format(f, S3_BUCKET))

        for f in glob.glob(BACKUP_RESTORE_DIR_DB_STREAM + "/*"):
            k = Key(bucket)
            k.key = f
            k.set_contents_from_filename(f)
            print("Stored {0} in S3 Bucket {1}\n\n".format(f, S3_BUCKET))

    except boto.exception.S3ResponseError as err:
        print(err)


def get_s3():
    """Get Backup from S3"""
    config = Config()
    conn = S3Connection(config.get("aws", "access_key_id"), config.get("aws", "secret_access_key"))

    try:
        bucket = conn.get_bucket(S3_BUCKET)
        for key in bucket.list():
            print(key.name)
            if BACKUP_RESTORE_DIR in key.name:
                if not os.path.exists(os.path.dirname(key.name)):
                    os.makedirs(os.path.dirname(key.name))
                key.get_contents_to_filename(key.name)
        print("Bucket Available in S3 - {0}".format(S3_BUCKET))
    except boto.exception.S3ResponseError as err:
        print(err)

def cmd(cmdList):
    process = subprocess.Popen(cmdList, stdout=subprocess.PIPE)
    print(process.stdout.read())

if __name__ == '__main__':
    backup()
    restore()