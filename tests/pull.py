#!/usr/bin/env python
from test_setup import *

b = s3.get_bucket('mocking')

for testname, teststring in dc_test_content.iteritems():
    k_cool = Key(b)
    k_cool.key = '%s.html' % testname
    content = k_cool.get_contents_as_string()
    assert content == dc_test_content[testname]
    print "pull", testname, "OK"

k_precise = Key(b)
k_precise.key = 'precise.txt'
try:
    content = k_precise.get_contents_as_string()
except boto.exception.S3ResponseError, e:
    print "pull successfully raised 404"

