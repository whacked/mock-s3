import os, sys, glob, fnmatch
import datetime

def modtime_of_path(p):
    return str(datetime.datetime.fromtimestamp(os.stat(p).st_mtime))

_BUCKETS_KEY = 'mock-s3:buckets'

class StrictRedis(object):
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

