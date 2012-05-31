#!/usr/bin/env python
import os

from test_setup import *

b = s3.get_bucket('mocking')

k_img = Key(b)
k_img.key = 'fake-image.txt'
src_size = len(k_img.get_contents_as_string())

dst_bucket = s3.create_bucket('backup')

dest_key_name = 'destination-key.txt'
# raises xml.sax._exceptions.SAXParseException
# although apparently successful
# related to http://code.google.com/p/boto/issues/detail?id=413
try:
    dst_bucket.copy_key(dest_key_name, 'mocking', 'fake-image.txt',)
except Exception, e:
    print "EXCEPTION:", e

k = Key(dst_bucket)
k.key = dest_key_name
assert len(k.get_contents_as_string()) == src_size
print "copy image OK"
