#!/usr/bin/env python
import os

from test_setup import *

b = s3.get_bucket('mocking')

dst_bucket = s3.create_bucket('backup')

# raises xml.sax._exceptions.SAXParseException
# although apparently successful
# related to http://code.google.com/p/boto/issues/detail?id=413
try:
    dst_bucket.copy_key('destination-key.txt', 'mocking', 'fake-image.txt',)
except Exception, e:
    print "EXCEPTION:", e

print "checking destination-key..."
k = Key(dst_bucket)
k.key = 'destination-key.txt'
print "filesize:", len(k.get_contents_as_string())
