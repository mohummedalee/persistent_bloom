# persistent_bloom
Bloom filter implementation in Python with json serialization/deserialization wrappers for easy interfacing with MongoDB.

Note 1: In the below readme, I refer to each Bloom filter as the **haystack** and the key to be inserted as the **pin**.

Note 2: Whatever DB you use should included a collection called **bloom**

There are two interfaces to the library:
## The Batch Loader: `bloom-batch.py`
Usage: `python bloom-batch.py <events_file>`
where `events_file` should be a csv with each line of the form: `haystackID, pin`

The environment variables you can change to configure this script are:
- DB_HOST
- DB_PORT

## The API: `bloom-api.py`
Usage: The two web services provided are:
- `exists/haystackID,pin`
- `add/haystackID,pin`
Can easily be called like `http://localhost:8888/add/55488b38f405c70300efceb2,powpow`
The API will maintain the log in a file called `bloom.log`.

Things configurable through environment variables:
- DB_HOST
- DB_PORT
- DB_NAME
- BLOOMAPI_PORT
- BLOOMAPI_ADDR

### Dependencies:
 - mmh3
 - bitarray
 - tornado

Unit tests are available in the *Tests* folder and can be run like: `python -m unittest Tests.test_name`
