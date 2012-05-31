#!/usr/bin/env python
import boto
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key

s3 = boto.connect_s3(host='localhost', port=10001, is_secure=False,
        calling_format = OrdinaryCallingFormat())


dc_test_content = {}
dc_test_content["cool"] = 'this is some really cool html'
dc_test_content["green"] = 'this is some really good music html'
dc_test_content["seminoles"] = 'this is some really seminoles html'
