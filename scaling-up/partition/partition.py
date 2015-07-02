__author__ = 'ssatpati'

import matplotlib.pyplot as plt
from crc32c import *
import os


def range_partition():
    d = {}
    with open("/usr/share/dict/words", "r") as f:
        for l in f:
            shard = l.lower()[0]
            if d.get(shard):
                d[shard].append(l)
            else:
                d[shard] = [l]

    shard_lengths = []
    for k, v in d.iteritems():
        shard_lengths.append(len(v))
        print(k, len(v))

    plt.bar(xrange(len(d)), shard_lengths, align='center')
    plt.xticks(xrange(len(d)), d.keys())

    output = "range-partition.pdf"
    if os.path.exists(output):
        os.remove(output)
    plt.savefig(output)
    #plt.show()


def consistent_hashing():
    d = {}
    with open("/usr/share/dict/words", "r") as f:
        for l in f:
            shard = crc(l) % 26
            if d.get(shard):
                d[shard].append(l)
            else:
                d[shard] = [l]

    shard_lengths = []
    for k, v in d.iteritems():
        shard_lengths.append(len(v))
        print(k, len(v))

    plt.bar(xrange(len(d)), shard_lengths, align='center')
    plt.xticks(xrange(len(d)), d.keys())

    output = "consistent-hasing.pdf"
    if os.path.exists(output):
        os.remove(output)
    plt.savefig(output)
    #plt.show()

if __name__ == '__main__':
    range_partition()
    consistent_hashing()
