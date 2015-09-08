import sys
import unittest
from math import ceil, log
from Bloom import Bloom

'''
These tests check if the Bloom filter functions well on its own
The API's tests are written in a different file
'''

# The capacity that will be used for testing (tweak only these and nothing else in the code)
testing_capacity = 500000
error_prob = 0.01

class BloomConstructionTests(unittest.TestCase):
    def setUp(self):
        self.filter = Bloom.Bloom(capacity=testing_capacity, error_rate=error_prob)

    def test_calculations(self):
        # Formulae taken from: http://hur.st/bloomfilter
        expected_bitsize = ceil((testing_capacity * log(error_prob)) / log(1.0 / (pow(2.0, log(2.0)))))
        expected_hashsize = round(log(2.0) * expected_bitsize / testing_capacity)

        self.assertEqual(self.filter.n, testing_capacity)
        self.assertEqual(self.filter.m, expected_bitsize)
        self.assertEqual(self.filter.k, expected_hashsize)
        self.assertEqual(self.filter.k, len(self.filter.seeds))
        self.assertEqual(self.filter.count, 0)

    def teardown(self):
        self.filter.dispose()
        self.filter = None

class BloomAddTests(unittest.TestCase):
    def setUp(self):
        self.filter = Bloom.Bloom(capacity=testing_capacity, error_rate=error_prob)

    def test_addition(self):
        self.filter.add('Parveen')
        self.filter.add('Samreen')

        self.assertTrue('Parveen' in self.filter)
        self.assertTrue('Samreen' in self.filter)
        self.assertFalse('Namkeen' in self.filter)

    def test_count(self):
        self.filter.add('Johny')
        self.assertEqual(self.filter.count, 1)
        self.filter.add('Rony')
        self.assertEqual(self.filter.count, 2)
        self.filter.add('Rony')
        self.assertEqual(self.filter.count, 2)
        self.filter.add('Bony')
        self.assertEqual(self.filter.count, 3)

    def teardown(self):
        self.filter.dispose()
        self.filter = None

class BloomSerializationTests(unittest.TestCase):
    def setUp(self):
        self.filter = Bloom.Bloom(capacity=testing_capacity, error_rate=error_prob)

    def test_serialization(self):
        self.filter.add('Parveen')
        self.filter.add('Samreen')

        serialized = Bloom.BloomSerializer.serialize(self.filter)
        reconstructed = Bloom.BloomSerializer.deserialize(serialized)

        # Confirm all fields are the same
        original = self.filter
        self.assertEqual(reconstructed.m, original.m)
        self.assertEqual(reconstructed.n, original.n)
        self.assertEqual(reconstructed.p, original.p)
        self.assertEqual(reconstructed.count, original.count)
        self.assertEqual(reconstructed.seeds, original.seeds)
        # I realize the size of the bitarray might be changed to be a multiple of 8
        # However, other than the right padded bits, everything else should remain the same
        self.assertTrue('Parveen' in reconstructed)
        self.assertTrue('Samreen' in reconstructed)
        self.assertFalse('Bogus' in reconstructed)

    def teardown(self):
        self.filter.dispose()
        self.filter = None

if __name__ == '__main__':
    # Run all tests
    unittest.main()
