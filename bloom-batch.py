import sys
import os
from Bloom import Bloom
from pymongo import MongoClient, errors
import csv
from bson.objectid import ObjectId
import time
import progressbar as pb

# MongoDB connection settings
# Local settings: {hostname: 'localhost', port: 27017}
hostname = os.environ.get('DB_HOST', 'localhost')
port = os.environ.get('DB_PORT', 27017)
new_filter_capacity = 500000

def insert(client, haystack, pin):
    db = client.patari
    result = db.bloom.find_one({'_id': ObjectId(haystack)})
    if result is not None:
        bf = Bloom.BloomSerializer.deserialize(result)
        bf.add(pin)
        serialized = Bloom.BloomSerializer.serialize(bf)
        serialized['_id'] = ObjectId(haystack)
        db.bloom.save(serialized)
    else:
        bf = Bloom.Bloom(new_filter_capacity)
        bf.add(pin)
        serialized = Bloom.BloomSerializer.serialize(bf)
        serialized['_id'] = ObjectId(haystack)
        db.bloom.save(serialized)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Please enter one csv file as a command line argumemt."
    else:
        print 'Opening events file...'
        try:
            fh = open(sys.argv[1], 'r')
        except:
            sys.exit('Could not open file ' + sys.argv[1])

        print 'Connecting to database...'
        try:
            client = MongoClient(hostname, port)
            client.server_info()
        except errors.ServerSelectionTimeoutError as err:
            sys.exit(err)

        # Initialize the progress bar
        num_lines = sum(1 for line in open(sys.argv[1], 'r'))
        _widgets = ['Progress: ', pb.Bar(), pb.Percentage()]
        progress = pb.ProgressBar(widgets=_widgets, maxval = num_lines)
        progvar = 0

        print 'Adding all events to database...'
        reader = csv.reader(fh)
        start = time.time()
        progress.start()
        for row in reader:
            progress.update(progvar+1)
            progvar += 1
            insert(client, row[0].strip(), row[1].strip())
        end = time.time()
        progress.finish()

        print ((end - start)/60), "minutes to add to database."
