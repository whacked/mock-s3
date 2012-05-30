#!/bin/bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=testsecret

#for TESTFILE in tests/*; do
#    echo $TESTFILE
#    python $TESTFILE
#done

# OK
python tests/list_buckets.py
python tests/create.py

python tests/key_list.py
#python tests/copy_image.py
#python tests/image_push.py
#python tests/pull.py
#python tests/push.py

#ipython
