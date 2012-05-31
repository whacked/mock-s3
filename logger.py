import logging

# http://docs.python.org/howto/logging-cookbook.html
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('mock_s3.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG) # .ERROR
# create formatter and add it to the handlers
formatter = logging.Formatter('[%(levelname)s] %(asctime)s\t%(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

