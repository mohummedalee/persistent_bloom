import os
import sys
import tornado.ioloop
import tornado.web
import tornado.httpserver
import json
from Bloom import Bloom
from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import logging

# MongoDB connection settings
# Local settings: {hostname: 'localhost', port: 27017, db_name = 'patari', api_port: 8888, api_addr:'localhost'}
new_filter_capacity = 500000

host = os.environ.get('DB_HOST', 'localhost')
port = os.environ.get('DB_PORT', 27017)
db_name = os.environ.get('DB_NAME', 'patari')
api_port = os.environ.get('BLOOMAPI_PORT', 8888)
api_addr = os.environ.get('BLOOMAPI_ADDR', 'localhost')

# Global connection and DB name
try:
    client = MongoClient(host, port)
    client.server_info()
except Exception as err:
    print "Couldn't start the API. Database not available."
    sys.exit(err)
db = client[db_name]

class BloomExists(tornado.web.RequestHandler):
    def get(self, haystack, pin):
        global db
        haystack = str(haystack)
        pin = str(pin)
        response = {}
        logger = logging.getLogger('bloom-api')

        if len(pin) != 24 or len(haystack) != 24:
            response['completed'] = 0
            response['description'] = "Incorrect inputs"
            logger.info('EXISTS - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:Bad inputs')
            self.write(json.dumps(response))
            self.finish()
        else:
            #print 'Received: Haystack:', haystack, ', Pin:', pin
            response['completed'] = 1
            response['description'] = 'Completed'

            # Check the existence
            try:
                result = db.bloom.find_one({'_id': ObjectId(haystack)})
            except:
                response['completed'] = 0
                response['description'] = "Couldn't connect to database"
                logger.error('EXISTS - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:DB down')
                self.write(json.dumps(response))
                self.finish()
                return

            if result is None:
                logger.info('EXISTS - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:New document')
                response['exists'] = 0
                bf = Bloom.Bloom(new_filter_capacity)
                serialized = Bloom.BloomSerializer.serialize(bf)
                serialized['_id'] = ObjectId(haystack)
                db.bloom.save(serialized)
            else:
                bf = Bloom.BloomSerializer.deserialize(result)
                if pin in bf:
                    response['exists'] = 1
                else:
                    response['exists'] = 0
                logger.info('EXISTS - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:' + str(response['exists']))

            self.write(json.dumps(response))
            self.finish


class BloomAdd(tornado.web.RequestHandler):
    def get(self, haystack, pin):
        global db
        haystack = str(haystack)
        pin = str(pin)
        response = {}
        logger = logging.getLogger('bloom-api')

        if len(pin) != 24 or (len(haystack)) != 24:
            response['completed'] = 0
            response['description'] = "Incorrect inputs"
            logger.info('ADD - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:Bad inputs')
            self.write(json.dumps(response))
            self.finish()
        else:
            response['completed'] = 1
            response['description'] = "Completed"

            # Pull out the bloom filter
            try:
                result = db.bloom.find_one({'_id': ObjectId(haystack)})
            except:
                response['completed'] = 0
                response['description'] = "Couldn't connect to database"
                logger.error('ADD - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:DB down')
                self.write(json.dumps(response))
                self.finish()
                return

            # Add the PinID
            if result is not None:
                bf = Bloom.BloomSerializer.deserialize(result)
                bf.add(pin)
                serialized = Bloom.BloomSerializer.serialize(bf)
                serialized['_id'] = ObjectId(haystack)
                db.bloom.save(serialized)
                logger.info('ADD - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:' + str(serialized['count']))
            else:
                bf = Bloom.Bloom(new_filter_capacity)
                bf.add(pin)
                serialized = Bloom.BloomSerializer.serialize(bf)
                serialized['_id'] = ObjectId(haystack)
                db.bloom.save(serialized)
                logger.info('ADD - Haystack:' + haystack + ' - Pin:' + pin + ' - Msg:New document')

            self.write(json.dumps(response))
            self.finish()

# Defining the routes
application = tornado.web.Application([
    (r"/add/(\w+),(\w+)", BloomAdd),
    (r"/exists/(\w+),(\w+)", BloomExists)
])

# Start the API
if __name__ == '__main__':
    # Turn off Tornado's default logging
    logging.getLogger('tornado.access').disabled = True

    # Set up custom logging
    logger = logging.getLogger('bloom-api')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s')
    log_file = 'bloom.log'
    log_fh = logging.FileHandler(log_file)
    log_fh.setLevel(logging.INFO)
    log_fh.setFormatter(formatter)
    # Have to log to the file
    logger.addHandler(log_fh)
    # Don't have to log to the console
    logger.propagate = False
    print 'Logging in: ' + log_file

    # Serve, slave. Serve.
    print 'Serving at ' + host + ':' + str(api_port) + '/'
    try:
        logger.info("Booted up")
        server = tornado.httpserver.HTTPServer(application)
        server.listen(api_port, address=api_addr)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        # Just to gracefully shutdown
        print '\nQuitting API'
        logger.info("Shutdown")
        sys.exit()
