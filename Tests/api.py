import sys
import os
import requests
import unittest
from math import ceil, log
from Bloom import Bloom
from pymongo import MongoClient
from bson.objectid import ObjectId

'''
These tests check if the API works fine
The Bloom Filter's tests are written in a different file
'''

# Tweak these if you may
testing_capacity = 500000
error_prob = 0.01

db_host = os.environ.get('DB_HOST', 'localhost')
db_port = os.environ.get('DB_PORT', 27017)
db_name = os.environ.get('DB_NAME', 'patari')
api_addr = os.environ.get('BLOOMAPI_ADDR', 'localhost')
api_port = os.environ.get('BLOOMAPI_PORT', 8888)

# Global connection
try:
    client = MongoClient(db_host, db_port)
    client.server_info()
except:
    sys.exit("Could not connect to database. Quitting.")
db = client[db_name]

class APITests(unittest.TestCase):
    def setUp(self):
        self.filter = Bloom.Bloom(capacity=testing_capacity, error_rate=error_prob)
        self.pin = 'ffffffffffffffffffffffff'
        self.haystack = '000000000000000000000000'

    def test_addition_manually(self):
        global db
        requests.get('http://'+api_addr+':'+str(api_port)+'/add/'+self.haystack+','+self.pin)

        # Deserialize the bloom filter and check if it has the pin
        fetched = db.bloom.find_one({'_id': ObjectId(self.haystack)})
        bf = Bloom.BloomSerializer.deserialize(fetched)
        self.assertTrue(self.pin in bf)

    def test_addition_api(self):
        requests.get('http://'+api_addr+':'+str(api_port)+'/add/'+self.haystack+','+self.pin)

        # Check through the exists API that the pin is added in the haystack
        r = requests.get('http://'+api_addr+':'+str(api_port)+'/exists/'+self.haystack+','+self.pin)
        r = r.json()
        self.assertEqual(r['exists'], 1)

    def test_idempotent(self):
        '''
        This test checks if adding the same pin multiple times retains the same count
        The add operation should essentially be idempotent
        '''
        global db
        requests.get('http://'+api_addr+':'+str(api_port)+'/add/'+self.haystack+','+self.pin)
        requests.get('http://'+api_addr+':'+str(api_port)+'/add/'+self.haystack+','+self.pin)
        requests.get('http://'+api_addr+':'+str(api_port)+'/add/'+self.haystack+','+self.pin)

        fetched = db.bloom.find_one({'_id': ObjectId(self.haystack)})
        bf = Bloom.BloomSerializer.deserialize(fetched)
        # The addition should have been done only once
        self.assertTrue(self.pin in bf)
        self.assertEqual(bf.count, 1)

    def test_exists_autoadd(self):
        '''
        The API is designed so that a call to exists with an unknown haystackID creates a new bloom filter
        This test checks that function.
        '''
        # Confirm that it doesn't exist already
        fetched = db.bloom.find_one({'_id': ObjectId(self.haystack)})
        self.assertTrue(fetched is None)

        # Call the API now
        requests.get('http://'+api_addr+':'+str(api_port)+'/exists/'+self.haystack+','+self.pin)
        fetched = db.bloom.find_one({'_id': ObjectId(self.haystack)})
        self.assertTrue(fetched is not None)
        self.assertEqual(fetched['count'], 0)

    def tearDown(self):
        global db
        removed = db.bloom.remove({'_id': ObjectId(self.haystack)})
        self.filter = None

if __name__ == '__main__':
    # Run all tests
    unittest.main()
