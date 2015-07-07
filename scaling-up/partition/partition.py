__author__ = 'ssatpati'

import matplotlib.pyplot as plt
from crc32c import *
import os


def range_partition():
    d = {}
    total = 0
    with open("/usr/share/dict/words", "r") as f:
        for l in f:
            total += 1
            shard = l.lower()[0]
            if d.get(shard):
                d[shard].append(l)
            else:
                d[shard] = [l]

    shard_lengths = []
    for k, v in d.iteritems():
        shard_lengths.append(len(v))
        print(k, len(v))

    print "Total Keys: ", total

    plt.bar(xrange(len(d)), shard_lengths, align='center')
    plt.xticks(xrange(len(d)), d.keys())

    output = "range-partition.pdf"
    if os.path.exists(output):
        os.remove(output)
    plt.savefig(output)
    #plt.show()


def consistent_hashing():
    d = {}
    range_per_shard = pow(2,32) / 26
    with open("/usr/share/dict/words", "r") as f:
        for l in f:
            shard = crc(l) // range_per_shard
            if d.get(shard):
                d[shard].append(l)
            else:
                d[shard] = [l]

    shard_lengths = []
    for k, v in d.iteritems():
        shard_lengths.append(len(v))
        #print(k, len(v))

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
