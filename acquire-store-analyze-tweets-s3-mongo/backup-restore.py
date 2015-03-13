#!/usr/bin/env python

import os
import subprocess
import datetime

__author__ = 'ssatpati'

BACKUP_RESTORE_DIR = "dump"


def backup():
    dt = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Assuming a dump dir exists in current dir
    if os.path.exists(BACKUP_RESTORE_DIR):
        os.rename(BACKUP_RESTORE_DIR, BACKUP_RESTORE_DIR + "_" + dt)

    proc = subprocess.Popen('pwd', stdout=subprocess.PIPE)
    print(proc.stdout.read())

    proc = subprocess.Popen(['mongodump', '--db', 'db_tweets'], stdout=subprocess.PIPE)
    print(proc.stdout.read())

    proc = subprocess.Popen(['mongodump', '--db', 'db_streamT'], stdout=subprocess.PIPE)
    print(proc.stdout.read())

def restore():
    pass



if __name__ == '__main__':
    backup()