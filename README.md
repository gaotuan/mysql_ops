# mysql_ops #
    通过python3 脚本自动化监控mysql
* * *
```
目前已经实现的功能：
1、根据配置项自动检查mysql进程
2、根据配置选项kill 长时间运行的sql
3、kill进程时对mysql 进行快照，并发送邮件通知
4、Check活动的session数量，达到配置项设定的阀值时告警通知，控制检查频率
5、Check活动的transaction数量，达到配置项设定的阀值时告警通知，控制检查频率
6、Check trx_rseg_history_len ，达到配置项设定的阀值时告警通知，控制检查频率
```

```
计划还需要实现的功能：
1、接入其他告警渠道
```
