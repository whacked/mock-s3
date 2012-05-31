#!/usr/bin/env python
from test_setup import *
import os

b = s3.get_bucket('mocking')

k_img = Key(b)
k_img.key = 'fake-image.txt'
filepath = os.path.join(os.path.dirname(__file__), 'fake-image.txt')
k_img.set_contents_from_filename(filepath)
