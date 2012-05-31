import md5
import os
import shutil
from datetime import datetime

from errors import BucketNotEmpty, NoSuchBucket
from models import Bucket, BucketQuery, S3Item


BUCKETS_KEY = 'mock-s3:buckets'
CONTENT_FILE = '.mocks3_content'

def get_modtime(*p):
    return str(datetime.fromtimestamp(os.stat(os.path.join(*p)).st_mtime))

class FileStore(object):
    def __init__(self, root, redis):
        if not os.path.exists(root):
            os.mkdir(root)
        self.root = root
        self.redis = redis

        self._dc = {}
        self.buckets = self.get_all_buckets()

    def get_bucket_folder(self, bucket_name):
        print " get_bucket_folder(self, bucket_name)"
        return os.path.join(self.root, bucket_name)

    def get_all_buckets(self):
        print " get_all_buckets(self)"
        buckets = []
        #bucket_list = self.redis.smembers(BUCKETS_KEY)
        #for bucket in bucket_list:
        #    bucket_data = bucket.split('|')
        #    buckets.append(Bucket(bucket_data[0], bucket_data[1]))
        buckets.extend(map(lambda p: Bucket(p, get_modtime(self.root, p)), os.listdir(self.root)))
        #for root, lsdir, lsfile in os.walk(self.root):
        #    for f in lsfile:
        #        self._dc[os.path.join(self.root, f)] = True
        return buckets

    def get_bucket(self, bucket_name):
        print " get_bucket(self, bucket_name)"
        for bucket in self.buckets:
            if bucket.name == bucket_name:
                return bucket
        return None

    def create_bucket(self, bucket_name):
        if bucket_name not in [bucket.name for bucket in self.buckets]:
            #self.redis.sadd(BUCKETS_KEY, '%s|%s' % (bucket_name, creation_date))
            bucket_dir = os.path.join(self.root, bucket_name)
            if os.path.exists(bucket_dir):
                creation_date = get_modtime(bucket_dir)
            else:
                os.makedirs(bucket_dir)
                creation_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z') 
                self._dc[bucket_name] = {}
                bucket = Bucket(bucket_name, creation_date)
                self.buckets.append(bucket)
        else:
            bucket = self.get_bucket(bucket_name)
        return bucket

    def delete_bucket(self, bucket_name):
        print " delete_bucket(self, bucket_name)"
        bucket = self.get_bucket(bucket_name)
        if not bucket:
            raise NoSuchBucket
        items = self.redis.keys(bucket_name + '/*')
        if items:
            raise BucketNotEmpty
        self.redis.srem(BUCKETS_KEY, bucket_name)
        os.rmdir(os.path.join(self.root, bucket_name))

    def get_all_keys(self, bucket, **kwargs):
        print " get_all_keys(self, bucket, **kwargs)"
        max_keys = kwargs['max_keys']
        pattern = '%s/%s*' % (bucket.name, kwargs['prefix'])
        keys = self.redis.keys(pattern)
        keys.sort()
        is_truncated = False
        if len(keys) > max_keys:
            keys = keys[:max_keys]
            is_truncated = True
        matches = []
        for key in keys:
            values = self.redis.hgetall(key)
            actual_key = key.partition('/')[2]
            matches.append(S3Item(actual_key, **values))

        return BucketQuery(bucket, matches, is_truncated, **kwargs)

    def get_item(self, bucket_name, item_name):
        print " get_item(self, bucket_name, item_name)"
        key_name = os.path.join(bucket_name, item_name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)

        filepath = os.path.join(self.root, bucket_name, item_name)
        content = open(filepath).read()
        metadata = {
            'content_type': 'application/octet-stream', # should read from mimetype
            'creation_date': get_modtime(filepath), # datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'md5': md5.md5(content).hexdigest(),
            'filename': filepath, # filename
            'size': len(content),
        }

        #metadata = self.redis.hgetall(key_name)
        #if not metadata:
        #    return None

        item = S3Item(key_name, **metadata)
        item.io = open(filepath, 'rb')

        return item

    def copy_item(self, src_bucket_name, src_name, bucket_name, name, handler):
        print " copy_item(self, src_bucket_name, src_name, bucket_name, name, handler)"
        src_key_name = os.path.join(src_bucket_name, src_name)
        print src_key_name
        src_dirname = os.path.join(self.root, src_key_name)
        src_filename = os.path.join(src_dirname, CONTENT_FILE)
        src_meta = self.redis.hgetall(src_key_name)
        print src_meta

        bucket = self.get_bucket(bucket_name)
        key_name = os.path.join(bucket.name, name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)

        self.redis.delete(key_name)
        print key_name, src_meta
        print "<>" * 20
        self.redis.hmset(key_name, src_meta)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        shutil.copy(src_filename, filename)

        return S3Item(key_name, **src_meta)

    def store_data(self, bucket, item_name, headers, data):
        print " store_data(self, bucket, item_name, headers, data)"
        key_name = os.path.join(bucket.name, item_name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)

        metadata = None
        if self.redis.exists(key_name):
            metadata = self.redis.hgetall(key_name)

        m = md5.new()

        lower_headers = {}
        for key in headers:
            lower_headers[key.lower()] = headers[key]
        headers = lower_headers

        size = int(headers['content-length'])
        m.update(data)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(filename, 'wb') as f:
            f.write(data)

        if metadata:
            metadata['md5'] = m.hexdigest()
            metadata['modified_date'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            metadata['content_type'] = headers['content-type']
            metadata['size'] = size
        else:
            metadata = {
                'content_type': headers['content-type'],
                'creation_date': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                'md5': m.hexdigest(),
                'filename': filename,
                'size': size,
            }
        self.redis.hmset(key_name, metadata)

        s3_item = S3Item(key, **metadata)
        s3_item.io = open(filename, 'rb')
        return s3_item

    def store_item(self, bucket, item_name, handler):
        print " store_item(self, bucket, item_name, handler)"
        key_name = os.path.join(bucket.name, item_name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)

        filepath = os.path.join(self.root, bucket.name, item_name)

        metadata = None
        #if self.redis.exists(key_name):
        #    metadata = self.redis.hgetall(key_name)

        m = md5.new()

        headers = {}
        for key in handler.headers:
            headers[key.lower()] = handler.headers[key]

        size = int(headers['content-length'])
        data = handler.rfile.read(size)
        #if not os.path.exists(dirname):
        #    os.makedirs(dirname)
        #with open(filename, 'wb') as f:
        #    f.write(data)

        with open(filepath, "wb") as f:
            f.write(data)

        m.update(data)
        if metadata:
            metadata['md5'] = m.hexdigest()
            metadata['modified_date'] = get_modtime(filepath) # datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            metadata['content_type'] = headers['content-type']
            metadata['size'] = size
        else:
            metadata = {
                'content_type': headers['content-type'],
                'creation_date': get_modtime(filepath), # datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                'md5': m.hexdigest(),
                'filename': filepath, # filename
                'size': size,
            }
        print metadata
        #self.redis.hmset(key_name, metadata)

        return S3Item(key, **metadata)

    def delete_item(self, bucket, item_name):
        print " delete_item(self, bucket, item_name)"
        key_name = os.path.join(bucket.name, item_name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)
        shutil.rmtree(filename)
        self.redis.delete(key_name)
