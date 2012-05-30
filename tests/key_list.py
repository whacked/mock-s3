#!/usr/bin/env python
from test_setup import *

b = s3.create_bucket('mocking')

keys = b.get_all_keys()
for key in keys:
    print repr(key)
