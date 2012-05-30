import os, sys, glob, fnmatch
import datetime

def modtime_of_path(p):
    return str(datetime.datetime.fromtimestamp(os.stat(p).st_mtime))

_BUCKETS_KEY = 'mock-s3:buckets'

class OldStrictRedis(object):
    """fake redis server for mock_s3. not strict at all"""

    def __init__(self):
        self._BASEDIR = len(sys.argv) > 1 and sys.argv[-1] or os.getcwd()
        print("starting fakeredis at %s" % self._BASEDIR)

        fullpath = lambda p: os.path.join(self._BASEDIR, p)
        ls_bucket_dir = [p for p in os.listdir(self._BASEDIR) if os.path.isdir(fullpath(p)) and not p.startswith(".")]
        self.dc_bucket = dict([(p, []) for p in ls_bucket_dir])
        self.ls_bucket = map(lambda p: ("%s|%s" % (p, modtime_of_path(fullpath(p))), []), ls_bucket_dir)

        # get whole directory structure
        self._dc = {_BUCKETS_KEY: {}}
        for bucket_dir in ls_bucket_dir:
            for root, lsdir, lsfile in os.walk(fullpath(bucket_dir)):
                self.dc_bucket[bucket_dir].extend(lsfile)
                for f in lsfile:
                    self._dc[_BUCKETS_KEY][os.path.join(self._BASEDIR, root, f)] = True
        print self._dc

    def smembers(self, BUCKETS_KEY):
        print "GOT KEY", BUCKETS_KEY
        if BUCKETS_KEY not in self._dc:
            return []
        return self._dc[BUCKETS_KEY].keys()

    def sadd(self, BUCKETS_KEY, creation_string): # '%s|%s' % (bucket_name, creation_date)
        bucket_name, creation_date = creation_string.split("|")
        print "CALLING --------------------------- sadd",bucket_name, creation_date
        if bucket_name not in self.dc_bucket:
            self.dc_bucket[bucket_name] = []
        if self.exists(bucket_name):
            return 0
        self._dc[BUCKETS_KEY][bucket_name] = {}
        return 1

    def srem(self, BUCKETS_KEY, bucket_name):
        print "CALLING --------------------------- srem", BUCKETS_KEY, bucket_name
        del self._dc[BUCKETS_KEY][bucket_name]
        return 0

    def keys(self, pattern): # bucket_name + '/*'
        bucket_name, matcher = pattern.split("/")
        print "CALLING --------------------------- keys", bucket_name, matcher
        return self._dc[_BUCKETS_KEY].keys()

    def hgetall(self, key_name):
        print "CALLING --------------------------- hgetall", key_name
        return {}

    def delete(self, key_name):
        print "CALLING --------------------------- delete", key_name
        del self._dc[_BUCKETS_KEY][key_name]

    def exists(self, key_name):
        print "CALLING --------------------------- exists", key_name
        return key_name in self._dc[_BUCKETS_KEY]

    def hmset(self, key_name, metadata):
        print "CALLING --------------------------- hmset", key_name, metadata
        self._dc[_BUCKETS_KEY][key_name] = metadata
        return True

# src: http://seeknuance.com/2012/02/18/replacing-redis-with-a-python-mock/
from collections import defaultdict

class MockRedisLock(object):
    """Poorly imitate a Redis lock object so unit tests can run on our Hudson CI server without
    needing a real Redis server."""
 
    def __init__(self, redis, name, timeout=None, sleep=0.1):
        """Initialize the object."""
 
        self.redis = redis
        self.name = name
        self.acquired_until = None
        self.timeout = timeout
        self.sleep = sleep
 
    def acquire(self, blocking=True):  # pylint: disable=R0201,W0613
        """Emulate acquire."""
 
        return True
 
    def release(self):   # pylint: disable=R0201
        """Emulate release."""
 
        return
 
class MockRedisPipeline(object):
    """Imitate a redis-python pipeline object so unit tests can run on our Hudson CI server without
    needing a real Redis server."""
 
    def __init__(self, redis):
        """Initialize the object."""
 
        self.redis = redis
 
    def execute(self):
        """Emulate the execute method. All piped commands are executed immediately in this mock, so
        this is a no-op."""
 
        pass
 
    def delete(self, key):
        """Emulate a pipelined delete."""
 
        # Call the MockRedis' delete method
        self.redis.delete(key)
        return self
 
    def srem(self, key, member):
        """Emulate a pipelined simple srem."""
 
        self.redis.redis[key].discard(member)
        return self
 
class MockRedis(object):
    """Imitate a Redis object so unit tests can run on our Hudson CI server without needing a real
    Redis server."""
 
    # The 'Redis' store
# TODO: check this
    redis = defaultdict(dict)
    #redis = defaultdict(set)
 
    def __init__(self):
        """Initialize the object."""
        pass
 
    def delete(self, key):  # pylint: disable=R0201
        """Emulate delete."""
 
        if key in MockRedis.redis:
            del MockRedis.redis[key]
 
    def exists(self, key):  # pylint: disable=R0201
        """Emulate get."""
 
        return key in MockRedis.redis
 
    def get(self, key):  # pylint: disable=R0201
        """Emulate get."""
 
        # Override the default dict
        result = '' if key not in MockRedis.redis else MockRedis.redis[key]
        return result
 
    def hget(self, hashkey, attribute):  # pylint: disable=R0201
        """Emulate hget."""
 
        # Return '' if the attribute does not exist
        result = MockRedis.redis[hashkey][attribute] if attribute in MockRedis.redis[hashkey] \
                 else ''
        return result
 
    def hgetall(self, hashkey):  # pylint: disable=R0201
        """Emulate hgetall."""
 
        return MockRedis.redis[hashkey]
 
    def hlen(self, hashkey):  # pylint: disable=R0201
        """Emulate hlen."""
 
        return len(MockRedis.redis[hashkey])
 
    def hmset(self, hashkey, value):  # pylint: disable=R0201
        """Emulate hmset."""
 
        # Iterate over every key:value in the value argument.
        for attributekey, attributevalue in value.items():
            MockRedis.redis[hashkey][attributekey] = attributevalue
 
    def hset(self, hashkey, attribute, value):  # pylint: disable=R0201
        """Emulate hset."""
 
        MockRedis.redis[hashkey][attribute] = value
 
    def keys(self, pattern):  # pylint: disable=R0201
        """Emulate keys."""
        import re
 
        # Make a regex out of pattern. The only special matching character we look for is '*'
        regex = '^' + pattern.replace('*', '.*') + '$'
 
        # Find every key that matches the pattern
        result = [key for key in MockRedis.redis.keys() if re.match(regex, key)]
 
        return result
 
    def lock(self, key, timeout=0, sleep=0):  # pylint: disable=W0613
        """Emulate lock."""
 
        return MockRedisLock(self, key)
 
    def pipeline(self):
        """Emulate a redis-python pipeline."""
 
        return MockRedisPipeline(self)
 
    def sadd(self, key, value):  # pylint: disable=R0201
        """Emulate sadd."""
 
        # Does the set at this key already exist?
        if key in MockRedis.redis:
            # Yes, add this to the set
            MockRedis.redis[key][value] = True
        else:
            # No, override the defaultdict's default and create the set
            MockRedis.redis[key] = {value: True}
 
    def smembers(self, key):  # pylint: disable=R0201
        """Emulate smembers."""
 
        return MockRedis.redis[key]
 
def mock_redis_client():
    """Mock common.util.redis_client so we can return a MockRedis object instead of a Redis
    object."""
    return MockRedis()

StrictRedis = mock_redis_client
