#!/bin/bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=testsecret

# OK
echo tests/list_buckets.py
python tests/list_buckets.py
echo tests/create.py
python tests/create.py
echo tests/key_list.py
python tests/key_list.py

echo tests/push.py
python tests/push.py
echo tests/pull.py
python tests/pull.py
echo tests/image_push.py
python tests/image_push.py
echo tests/copy_image.py
python tests/copy_image.py

