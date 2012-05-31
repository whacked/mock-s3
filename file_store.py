import os, fnmatch
import shutil, hashlib
from datetime import datetime

from errors import BucketNotEmpty, NoSuchBucket
from models import Bucket, BucketQuery, S3Item


BUCKETS_KEY = 'mock-s3:buckets'
CONTENT_FILE = '.mocks3_content'

def get_modtime(*p):
    return str(datetime.fromtimestamp(os.stat(os.path.join(*p)).st_mtime))

def get_metadata(filepath, **kw):
    rtn = {"filename": filepath,
           'content_type': kw.get('content_type', 'application/octet-stream'), # should read from mimetype
           'creation_date': kw.get('creation_date', get_modtime(filepath)),
            }
    if "md5" in kw and "size" in kw:
        rtn.update({"md5": kw["md5"], "size": kw["size"]})
    else:
        content = open(filepath).read()
        rtn["md5"] = hashlib.md5(content).hexdigest()
        rtn["size"] = len(content)
    return rtn

class FileStore(object):
    

    def __init__(self, root):
        if not os.path.exists(root):
            os.mkdir(root)
        self.root = root

        self._dc = {}
        # pre-populate keys if necessary
        for bucket in self.get_all_buckets():
            self._dc[bucket.name] = {}
            for root, lsdir, lsfile in os.walk(os.path.join(self.root, bucket.name)):
                for f in lsfile:
                    self._dc[bucket.name][f] = get_metadata(os.path.join(self.root, bucket.name, f))

    def get_bucket_folder(self, bucket_name):
        print " get_bucket_folder(self, bucket_name)"
        return os.path.join(self.root, bucket_name)

    def get_all_buckets(self):
        print " get_all_buckets(self)"
        return map(lambda p: Bucket(p, get_modtime(self.root, p)), os.listdir(self.root))

    def get_bucket(self, bucket_name):
        print " get_bucket(self, bucket_name)"
        if bucket_name in self._dc:
            return Bucket(bucket_name, get_modtime(os.path.join(self.root, bucket_name)))
        return None

    def create_bucket(self, bucket_name):
        if bucket_name not in self._dc:
            bucket_dir = os.path.join(self.root, bucket_name)
            if os.path.exists(bucket_dir):
                creation_date = get_modtime(bucket_dir)
            else:
                os.makedirs(bucket_dir)
                creation_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z') 
            bucket = Bucket(bucket_name, creation_date)
            self._dc[bucket_name] = {}
        else:
            bucket = self.get_bucket(bucket_name)
        return bucket

    def delete_bucket(self, bucket_name):
        print " delete_bucket(self, bucket_name)"
        bucket = self.get_bucket(bucket_name)
        if not bucket:
            raise NoSuchBucket
        items = self.get_all_keys(bucket, pattern = "*") # redis.keys(bucket_name + '/*')
        if items:
            raise BucketNotEmpty
        del self._dc[bucket_name]
        os.rmdir(os.path.join(self.root, bucket_name))

    def get_all_keys(self, bucket, **kwargs):
        print " get_all_keys(self, bucket, **kwargs)"
        max_keys = kwargs['max_keys']
        pattern = '%s/%s*' % (bucket.name, kwargs['prefix'])
        pattern = "%s*" % kwargs["prefix"]

        keys = filter(lambda k: fnmatch.fnmatch(k, pattern), self._dc[bucket.name].keys())
        keys.sort()
        print self._dc

        is_truncated = False
        if len(keys) > max_keys:
            keys = keys[:max_keys]
            is_truncated = True
        matches = []
        for key in keys:
            print "FOUND KEY", key
            metadata = get_metadata(os.path.join(self.root, bucket.name, key))
            matches.append(S3Item(key, **metadata))

        return BucketQuery(bucket, matches, is_truncated, **kwargs)

    def get_item(self, bucket_name, item_name):
        print " get_item(self, bucket_name, item_name)"
        key_name = os.path.join(bucket_name, item_name)
        #dirname = os.path.join(self.root, key_name)
        #filename = os.path.join(dirname, CONTENT_FILE)

        filepath = os.path.join(self.root, bucket_name, item_name)
        if not os.path.exists(filepath):
            return None
        metadata = get_metadata(filepath)
        #if not metadata:
        #    return None

        item = S3Item(key_name, **metadata)
        item.io = open(filepath, 'rb')

        return item

    def copy_item(self, src_bucket_name, src_name, tgt_bucket_name, tgt_name, handler):
        print " copy_item(self, src_bucket_name = %s, src_name = %s, tgt_bucket_name = %s, tgt_name = %s, handler)" % (src_bucket_name, src_name, tgt_bucket_name, tgt_name)
        src_key_name = os.path.join(src_bucket_name, src_name)
        print src_key_name
        src_dirname = os.path.join(self.root, src_key_name)
        src_filename = os.path.join(src_dirname, CONTENT_FILE)
        src_meta = self._dc[src_bucket_name][src_name]

        tgt_bucket = self.get_bucket(tgt_bucket_name)
        key_name = os.path.join(tgt_bucket.name, tgt_name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)

        src_filepath = os.path.join(self.root, src_bucket_name, src_name)
        tgt_filepath = os.path.join(self.root, tgt_bucket.name, tgt_name)

        tgt_bucket_dir = os.path.join(self.root, tgt_bucket.name)
        if not os.path.exists(tgt_bucket_dir):
            os.mkdir(tgt_bucket_dir)
        #shutil.copy(src_filename, filename)
        print src_filepath , ">>>>>>>>", tgt_filepath
        shutil.copy(src_filepath, tgt_filepath)
        self._dc[tgt_bucket.name][tgt_name] = get_metadata(tgt_filepath)

        return S3Item(tgt_name, **src_meta)

    def store_data(self, bucket, item_name, headers, data):
        print " store_data(self, bucket, item_name, headers, data)"
        key_name = os.path.join(bucket.name, item_name)
        dirname = os.path.join(self.root, key_name)
        filename = os.path.join(dirname, CONTENT_FILE)

        filepath = os.path.join(self.root, bucket.name, item_name)

        lower_headers = {}
        for key in headers:
            lower_headers[key.lower()] = headers[key]
        headers = lower_headers

        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(filename, 'wb') as f:
            f.write(data)

        self._dc[bucket.name][key_name] = get_metadata(filepath,
                creation_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                md5 = hashlib.md5(data).hexdigest(),
                size = int(headers['content-length']),
                )

        s3_item = S3Item(key, **metadata)
        s3_item.io = open(filename, 'rb')
        return s3_item

    def store_item(self, bucket, item_name, handler):
        print " store_item(self, bucket = %s, item_name = %s, handler)" % (bucket.name, item_name)

        filepath = os.path.join(self.root, bucket.name, item_name)

        headers = {}
        for key in handler.headers:
            headers[key.lower()] = handler.headers[key]

        size = int(headers['content-length'])
        data = handler.rfile.read(size)
        print "GOT DATA", "= " * 20
        print data
        print " = = = = =" * 20
        print hashlib.md5(data).hexdigest()

        with open(filepath, "wb") as f:
            f.write(data)

        metadata = get_metadata(filepath,
                content_type = headers['content-type'],
                creation_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                md5 = hashlib.md5(data).hexdigest(),
                size = int(headers['content-length']),
                )
        self._dc[bucket.name][item_name] = metadata

        return S3Item(key, **metadata)

    def delete_item(self, bucket, item_name):
        print " delete_item(self, bucket, item_name)"
        os.unlink(os.path.join(self.root, bucket.name, item_name))
        del self._dc[bucket.name][item_name]
