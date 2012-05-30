#!/usr/bin/env python
from test_setup import *


b = s3.create_bucket('mocking')

###kwrite = Key(b)
###kwrite.key = 'hello.html'
###kwrite.set_contents_from_string('this is some really cool html')
###
###kread = Key(b)
###kread.key = 'hello.html'
###content  = kread.get_contents_as_string()
###print content
