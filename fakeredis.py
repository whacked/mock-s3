


class StrictRedis(object):
    """fake redis server for mock_s3. not strict at all"""

    def smembers(self, BUCKETS_KEY):
        return []

    def sadd(self, BUCKETS_KEY, str_FIXME): # '%s|%s' % (bucket_name, creation_date)
        pass

    def keys(self, str_FIXME): # bucket_name + '/*'
        pass

    def srem(self, BUCKETS_KEY, bucket_name):
        pass

    def keys(self, pattern):
        pass

    def hgetall(self, key):
        pass

    def hgetall(self, key_name):
        pass

    def hgetall(self, src_key_name):
        pass

    def delete(self, key_name):
        pass

    def hmset(self, key_name, src_meta):
        pass

    def exists(self, key_name):
        pass

    def hgetall(self, key_name):
        pass

    def hmset(self, key_name, metadata):
        pass

    def exists(self, key_name):
        pass

    def hgetall(self, key_name):
        pass

    def hmset(self, key_name, metadata):
        pass

    def delete(self, key_name):
        pass

