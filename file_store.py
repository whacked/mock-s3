import os, fnmatch
import shutil, hashlib
from datetime import datetime

from errors import BucketNotEmpty, NoSuchBucket
from models import Bucket, BucketQuery, S3Item

from logger import logger
import mimetypes

BUCKETS_KEY = 'mock-s3:buckets'
# for files where we cannot guess the mimetype,
# return anything above this size as application/octet-stream
# return anything below this size as text/plain
THRESHOLD_DOWNLOAD = 10**5

def get_modtime(*p):
    return str(datetime.fromtimestamp(os.stat(os.path.join(*p)).st_mtime))

def get_metadata(filepath, **kw):
    rtn = {"filename": filepath,
           'creation_date': kw.get('creation_date', get_modtime(filepath)),
            }
    if "md5" in kw and "size" in kw:
        rtn.update({"md5": kw["md5"], "size": kw["size"]})
    else:
        content = open(filepath).read()
        rtn["md5"] = hashlib.md5(content).hexdigest()
        rtn["size"] = len(content)

    guess_mimetype = kw.get('content_type') or mimetypes.guess_type(filepath)[0]
    if guess_mimetype is None:
        if rtn["size"] < THRESHOLD_DOWNLOAD:
            guess_mimetype = "text/plain"
        else:
            guess_mimetype = "application/octet-stream"

    rtn['content_type'] = kw.get('content_type', ), # should read from mimetype
    return rtn

class FileStore(object):
    
    def populate_keys(self):
        for bucket in self.get_all_buckets():
            self._dc[bucket.name] = {}
            # root directory is special case
            if bucket.name == ".":
                lsfile = filter(lambda f: not fnmatch.fnmatch(f, self.key_excluder), os.listdir(self.root))
                for key in lsfile:
                    filepath = os.path.join(self.root, key)
                    if os.path.isdir(filepath): continue
                    self._dc[bucket.name][key] = get_metadata(filepath)
                continue
            for root, lsdir, lsfile in os.walk(os.path.join(self.root, bucket.name)):
                for f in lsfile:
                    filepath = os.path.join(root, f)
                    key = filepath[len(os.path.join(self.root, bucket.name))+1:]
                    if fnmatch.fnmatch(key, self.key_excluder):
                        continue
                    self._dc[bucket.name][key] = get_metadata(filepath)

    def __init__(self, root, bucket_excluder = None, key_excluder = None):
        if not os.path.exists(root):
            os.mkdir(root)
        self.root = root
        self.bucket_excluder = bucket_excluder
        self.key_excluder = key_excluder

        self._dc = {}
        # pre-populate keys if necessary
        self.populate_keys()

    def get_bucket_folder(self, bucket_name):
        logger.info(" get_bucket_folder(self, bucket_name)")
        return os.path.join(self.root, bucket_name)

    def get_all_buckets(self):
        logger.info(" get_all_buckets(self)")
        # force-add root directory "."
        return map(lambda p: Bucket(p, get_modtime(self.root, p)), ["."] + [d for d in os.listdir(self.root) if os.path.isdir(os.path.join(self.root, d)) and not fnmatch.fnmatch(d, self.bucket_excluder)])

    def get_bucket(self, bucket_name):
        logger.info(" get_bucket(self, bucket_name)")
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
        logger.info(" delete_bucket(self, bucket_name)")
        bucket = self.get_bucket(bucket_name)
        if not bucket:
            raise NoSuchBucket
        items = self.get_all_keys(bucket, pattern = "*") # redis.keys(bucket_name + '/*')
        if items:
            raise BucketNotEmpty
        del self._dc[bucket_name]
        os.rmdir(os.path.join(self.root, bucket_name))

    def get_all_keys(self, bucket, **kwargs):
        logger.info(" get_all_keys(self, bucket, **kwargs)")
        max_keys = kwargs['max_keys']
        pattern = '%s/%s*' % (bucket.name, kwargs['prefix'])
        pattern = "%s*" % kwargs["prefix"]

        keys = filter(lambda k: fnmatch.fnmatch(k, pattern), self._dc[bucket.name].keys())
        keys.sort()

        is_truncated = False
        if len(keys) > max_keys:
            keys = keys[:max_keys]
            is_truncated = True
        matches = []
        for key in keys:
            metadata = get_metadata(os.path.join(self.root, bucket.name, key))
            matches.append(S3Item(key, **metadata))

        return BucketQuery(bucket, matches, is_truncated, **kwargs)

    def get_item(self, bucket_name, item_name):
        logger.info(" get_item(self, bucket_name, item_name)")

        filepath = os.path.join(self.root, bucket_name, item_name)
        if not os.path.exists(filepath):
            return None
        metadata = get_metadata(filepath)
        #if not metadata:
        #    return None

        item = S3Item(item_name, **metadata)
        item.io = open(filepath, 'rb')

        return item

    def copy_item(self, src_bucket_name, src_name, tgt_bucket_name, tgt_name, handler):
        logger.info(" copy_item(self, src_bucket_name = %s, src_name = %s, tgt_bucket_name = %s, tgt_name = %s, handler)" % (src_bucket_name, src_name, tgt_bucket_name, tgt_name))
        src_key_name = os.path.join(src_bucket_name, src_name)
        
        src_meta = self._dc[src_bucket_name][src_name]

        tgt_bucket = self.get_bucket(tgt_bucket_name)

        src_filepath = os.path.join(self.root, src_bucket_name, src_name)
        tgt_filepath = os.path.join(self.root, tgt_bucket.name, tgt_name)

        tgt_bucket_dir = os.path.join(self.root, tgt_bucket.name)
        if not os.path.exists(tgt_bucket_dir):
            os.mkdir(tgt_bucket_dir)
        
        logger.info("COPY: %s ---> %s" % (src_filepath , tgt_filepath))
        shutil.copy(src_filepath, tgt_filepath)
        self._dc[tgt_bucket.name][tgt_name] = get_metadata(tgt_filepath)

        return S3Item(tgt_name, **src_meta)

    def store_data(self, bucket, item_name, headers, data):
        logger.info(" store_data(self, bucket, item_name, headers, data)")

        filepath = os.path.join(self.root, bucket.name, item_name)

        lower_headers = {}
        for key in headers:
            lower_headers[key.lower()] = headers[key]
        headers = lower_headers

        with open(filename, 'wb') as f:
            f.write(data)

        self._dc[bucket.name][item_name] = get_metadata(filepath,
                creation_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                md5 = hashlib.md5(data).hexdigest(),
                size = int(headers['content-length']),
                )

        s3_item = S3Item(key, **metadata)
        s3_item.io = open(filename, 'rb')
        return s3_item

    def store_item(self, bucket, item_name, handler):
        logger.info(" store_item(self, bucket = %s, item_name = %s, handler)" % (bucket.name, item_name))

        filepath = os.path.join(self.root, bucket.name, item_name)

        headers = {}
        for key in handler.headers:
            headers[key.lower()] = handler.headers[key]

        size = int(headers['content-length'])
        data = handler.rfile.read(size)

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
        logger.info(" delete_item(self, bucket, item_name)")
        os.unlink(os.path.join(self.root, bucket.name, item_name))
        del self._dc[bucket.name][item_name]
