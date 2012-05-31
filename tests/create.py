#!/usr/bin/env python
from test_setup import *


b = s3.create_bucket('mocking')
found = False
for bucket in s3.get_all_buckets():
    print bucket
    if bucket.name == "mocking":
        found = True
assert found
print "create bucket OK"

kwrite = Key(b)
kwrite.key = 'hello.html'
test_string = 'this is some really cool html'
rtn = kwrite.set_contents_from_string(test_string)
assert rtn == len(test_string)
print "write OK"

kread = Key(b)
kread.key = 'hello.html'
content  = kread.get_contents_as_string()
assert content == test_string
print "read OK"
