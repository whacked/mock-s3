#!/usr/bin/env python
import boto
from boto.s3.key import Key

s3 = boto.connect_s3(host='localhost', port=10001, is_secure=False)
# boto.s3.connection doesn't seem to become available
# until after you attempt a connection
calling_format = boto.s3.connection.OrdinaryCallingFormat()
s3 = boto.connect_s3(host='localhost', port=10001, is_secure=False,
        calling_format = calling_format)
