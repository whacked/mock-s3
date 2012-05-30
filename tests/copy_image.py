#!/usr/bin/env python
import os

from test_setup import *

#b = s3.get_bucket('mocking')

dst_bucket = s3.create_bucket('backup')

dst_bucket.copy_key('/etc/alternatives/start-here-24.png', 'mocking', '/etc/alternatives/start-here-24.png',)
