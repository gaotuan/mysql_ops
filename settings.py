__author__ = 'gaott'

DB_AUTH = "xx:xx","xxxx:xx"

# configuration file read interval, 10s
CHECK_CONFIG_INTERVAL = 20

# mysql connection ping interval to keepalive
# 10 times * 10 = 100 seconds. DO NOT exceed *wait_timeout*
CHECK_PING_MULTI = 10
CONFIG_FILE_PATH = 'mysqk.ini'