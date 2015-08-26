from __future__ import division
from math import log, ceil
from bitarray import bitarray
import mmh3
from json import dumps
from math import ceil
from bson.binary import Binary

class BloomSerializer:
    @staticmethod
    def serialize(obj):
        '''
        Given a Bloom filter, serializes it into JSON which goes into MongoDB
        '''
        ret = obj.__dict__.copy()
        # Make bitarray BSON compatible
        ret['bitarray'] = Binary(obj.bitarray.tobytes())
        return ret

    @staticmethod
    def deserialize(dic):
        '''
        Given a JSON dictionary, returns a bloom filter object
        '''
        # I enjoy giving covert dirty names to my variables
        bf = Bloom(capacity=dic['n'], error_rate=dic['p'])
        # bf.m should be calculated automatically
        # I assume len(dic['seeds']) == bf.k
        bf.count = dic['count']
        bf.seeds = dic['seeds']
        bf.bitarray = bitarray()
        bf.bitarray.frombytes(dic['bitarray'])
        return bf

class Bloom(object):
    def __init__(self, capacity, error_rate=0.01):
        assert capacity > 1
        self.n = int(capacity)
        self.p = error_rate
        self.count = 0

        # How many bits needed. Below formula must return positive value
        # m = (n x ln(p)) / (ln2)^2
        self.m = int(ceil(abs((self.n * log(self.p)) / log(2)**2)))
        self.bitarray = bitarray(self.m)

        # How many hashes needed
        # k = ln2 x (m/n)
        self.k = int(ceil((self.m/self.n) * log(2)))
        self.seeds = []

        # Seed the k murmur hashes
        for i in range(self.k):
            self.seeds.append(i)

    def get_hashes(self, key):
        """
        Returns the output of the k hash functions packed into a list
        """
        hashes = []
        for i in range(self.k):
            hashes.append(mmh3.hash(key, self.seeds[i]) % self.m)

        return hashes

    def add(self, key):
        """
        Adds the given key to the bloomfilter, updates the count variable as well
        """
        hashes = self.get_hashes(str(key))
        exists = True

        for idx in hashes:
            # Determinging if this was added before
            if not self.bitarray[idx]:
                exists = False
            self.bitarray[idx] = 1

        if not exists:
            self.count += 1

    def __contains__(self, key):
        """
        Checks if key is part of the bloom filter
        """
        hashes = self.get_hashes(str(key))

        for idx in hashes:
            if not self.bitarray[idx]:
                return False
        return True

    def filter_stats(self):
        # Print a bunch of stuff related to the bloom filter
        print "Capacity: ", self.n
        print "Number of bits: ", self.m
        print "Number of hashes: ", self.k
