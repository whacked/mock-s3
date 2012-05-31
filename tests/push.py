#!/usr/bin/env python
from test_setup import *

b = s3.get_bucket('mocking')

for testname, teststring in dc_test_content.iteritems():
    k_cool = Key(b)
    k_cool.key = '%s.html' % testname
    set_len = k_cool.set_contents_from_string(dc_test_content[testname])
    assert set_len == len(dc_test_content[testname])
    print "push", testname, "OK"

