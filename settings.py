__author__ = 'gaott'

DB_AUTH = "xydba:f42afdc42333d0de07a5b9","xy_review_read:e0cdae7ff689ba63da","xx:f277ff91a9d24c45eb8a1042d79102dc50e5be07"

# configuration file read interval, 10s
CHECK_CONFIG_INTERVAL = 20

# mysql connection ping interval to keepalive
# 10 times * 10 = 100 seconds. DO NOT exceed *wait_timeout*
CHECK_PING_MULTI = 10
CONFIG_FILE_PATH = 'mysqk.ini'