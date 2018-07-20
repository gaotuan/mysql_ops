__author__ = 'gaott'

DB_AUTH = {"gaotuan":"72e8e02b4abeee2317856b8a232efb6e"}


# configuration file read interval, 10s
CHECK_CONFIG_INTERVAL = 2

# mysql connection ping interval to keepalive
# 10 times * 10 = 100 seconds. DO NOT exceed *wait_timeout*
CHECK_PING_MULTI = 5
CONFIG_FILE_PATH = 'mysqk.ini'