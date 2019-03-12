
# coding: utf-8
#/usr/bin/env python3
import pymysql
import os, sys, time, datetime
import subprocess
import configparser
import threading
from threading import Thread, local
import re
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import json
from collections import defaultdict
import settings
import prpcryptec
from warnings import filterwarnings, resetwarnings
from logging.handlers import TimedRotatingFileHandler
import snapshot_report
import traceback

LOG_FILE = 'killquery.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(levelname)s:%(asctime)s--%(thread)s>>::%(message)s',level=logging.DEBUG)
#handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024*1024, backupCount=5)
handler = TimedRotatingFileHandler(LOG_FILE, when='d', interval=1, backupCount=7)
formatter = logging.Formatter('%(asctime)s [%(levelname)-7s] %(threadName)6s >> %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
#work_dir='D:\\deploy\\test3\\mykill\\'
work_dir='/home/tuantuangao/mysql_ops/tmpfile/'


THREAD_DATA = local()
KEY_DB_AUTH = "1111111111111111"

# get configuration section
# db_commkill: common config and can be overwritten (inherit)
# db_commconfig: common info and not inherit
def get_setttings(sect, opt=''):
    cf = configparser.ConfigParser()
    cf.read(settings.CONFIG_FILE_PATH)

    if opt != '':
        o = cf.get(sect,opt)
        return o

    if re.match('id_', sect):
        v1 = dict(cf.items("db_commkill"))
        try:
            v2 = dict(cf.items(sect))
        except configparser.NoSectionError:
            logger.debug("no section %s found in %s, use comm section", sect, settings.CONFIG_FILE_PATH)
            v2 = v1

        v2 = dict(v1, **v2)

        if 'k_user' in v2:
            k_users = v2['k_user'].replace(' ', '').split(',')
            v2['k_user'] = k_users

        # 匹配和排除规则，转化成python regex object
        if 'k_exclude' in v2:
            k_exclude = re.compile(v2['k_exclude'])
            v2['k_exclude'] = k_exclude
        if 'k_include' in v2:
            # print v2['k_include']
            k_include = re.compile(v2['k_include'])
            v2['k_include'] = k_include
    else:
        v2 = dict(cf.items(sect))

        # 运行的时间窗口，取得开始和结束时间
        if 'run_time_window' in v2:
            run_time_window = v2['run_time_window'].replace(' ', '').split('-')
            if len(run_time_window) != 2:
                v2['run_time_window'] = []
            else:
                v2['run_time_window'] = run_time_window
    print ("return the setting:" ,v2)
    return v2

# get processlist to check connection session
#
def get_processlist_kthreads(conn, kill_opt, db_id):
    if conn is None:
        logger.error("get db connection error %s")

    processlist_file = work_dir+'processlist_' + db_id + '.txt'
    logger.debug("get the information_schema.processlist on this moment: %s", processlist_file)

    threads_tokill = defaultdict(list)
    try:
        cur = conn.cursor()
        sqlstr = "select * from information_schema.processlist  where info is not null  order by time desc;"

        cur.execute(sqlstr)
        rs = cur.fetchall()

    except Exception as   e:
        logger.critical("Get processlist connection error. Wait ping alive to reconnect.")
    else:
        fo = open(processlist_file, "w")
        fo.write("\n\n################  " + time.asctime() + "  ################\n")
        fo.write("""
            <style> .mytable,.mytable th,.mytable td {
                font-size:0.8em;    text-align:left;    padding:4px;    border-collapse:collapse;
            } </style>
            <table class='mytable'> <tr><th>thread_id</th><th>user</th><th>host</th><th>db</th><th>command</th><th>time</th><th>state</th><th>info</th></tr> 
        """)

        logger.info("check this conn thread according to kill_opt one by one")

        for row in rs:
            iskill_thread = kill_judge(row, kill_opt)
            if iskill_thread > 0:
                threads_tokill[row[1]].append(iskill_thread)

                fo.write("<tr><td>" + "</td> <td>".join(map(str, row)) + "</td></tr>\n")
            # print str(row)
        fo.write("</table>")
        fo.close()
    finally:
        cur.close()

    return threads_tokill

#get active session
def get_active_session(conn, db_id):
    processlist_file = work_dir+'processlist_' + db_id + '.txt'
    logger.debug("get active session from information_schema.processlist on this moment: %s", processlist_file)
    try:
        cur = conn.cursor()
        sqlstr = "select   count(*),max(time),concat(SUBSTR(REPLACE(REPLACE(REPLACE(info, CHAR(10), ''), CHAR(13), ''),'  ',' ' )FROM 1 FOR 30),'||', REPLACE(REPLACE(substr(info,LOCATE('from',info)+4,30), CHAR(10), ''), CHAR(13), ''))  sub  " \
                 "from information_schema.PROCESSLIST where info is not null  and id not in( select connection_id() )  " \
                 "group by concat(SUBSTR(REPLACE(REPLACE(REPLACE(info, CHAR(10), ''), CHAR(13), ''),'  ',' ' )FROM 1 FOR 30),'||', REPLACE(REPLACE(substr(info,LOCATE('from',info)+4,30), CHAR(10), ''), CHAR(13), '')) order by 1;"
        cur.execute(sqlstr)
        rs = cur.fetchall()
        fo=open(processlist_file,"w")
        fo.write("\n\n################  " + time.asctime() + "  ################\n")
        fo.write("""
            <style> .mytable,.mytable th,.mytable td {
                font-size:0.8em;    text-align:left;    padding:4px;    border-collapse:collapse;
            } </style>
            <table class='mytable'> <tr><th>count</th><th>max_time</th><th>sub_sql</th></tr> 
        """)
        for row in rs:
            fo.write("<tr><td>" + "</td> <td>".join(map(str, row)) + "</td></tr>\n")
        fo.write("</table>")
    except Exception as e:
        logger.critical("Get active processlist  error:$s",e)
    finally:
        cur.close()
        fo.close()




def db_reconnect(db_user, db_id):
    logger.debug("beging reconnect....")
    db_pass = settings.DB_AUTH[db_user]
    try:
        pc = prpcryptec.PrpCrypt(KEY_DB_AUTH)
        v_pass=pc.decrypt(db_pass)
    except Exception as a:
        logger.error("prpcryptec error in reconnect: %s",a)

    db_instance = get_setttings("db_info", db_id)
    db_host, db_port = db_instance.replace(' ', '').split(':')

    db_conn = None

    while not db_conn:
        try:
            logger.debug("Reconnect Database %s: host='%s', user='%s, port=%s", db_id, db_host, db_user, db_port)
            db_conn = pymysql.connect(host=db_host, user=db_user, passwd=v_pass,database='', port=int(db_port))

        except pymysql.err.MySQLError  as  e:

            if e.args[0] in (2013, 2003):
                logger.critical('Error find in reconnecting %d: %s', e.args[0], e.args[1])
                logger.critical("Reconnect Error wait next time: Database %s: host='%s', user='%s, port=%s", db_id, db_host, db_user, db_port)

        except Exception as inst:
            logger.error('Reconnect error exit :%s',inst)
            return

        time.sleep(10)

    return db_conn


# judge this thread meet kill_opt or not
def kill_judge(row, kill_opt):
    if (row[1] in kill_opt['k_user'] or 'all' in kill_opt['k_user']) \
            and not kill_opt['k_exclude'].search(str(row)):  # exclude have high priority

        if kill_opt['k_include'].search(str(row)):
            return int(row[0])

        if int(kill_opt['k_sleep']) == 0 and row[4] == 'Sleep':
            return 0
        elif 0 < int(kill_opt['k_sleep']) < row[5] and row[4] == 'Sleep':
            return int(row[0])
        elif row[4] != 'Sleep':
            if 0 < int(kill_opt['k_longtime']) < row[5]:
                #if row[1] not in settings.DB_AUTH.keys():
                #    logger.debug("You may have set all users to kill, but %s is not in DB_AUTH list. Skip thread %d : %s ", row[1], row[0], row[7])
                #    return 0
                return int(row[0])
        return 0
    else:
        return 0


# take snapshot to gather more info before kill
def get_more_info(conn, threadName):
    logger.info("Gather info before kill using the same connection START")

    str_fulllist = "select * from information_schema.processlist "
    str_status = "show engine innodb status"
    str_trx_lockwait = """
        SELECT
            tx.trx_id, 'Blocker' role, p.id thread_id, p.`USER` dbuser,
            LEFT (p.`HOST`, locate(':', p.`HOST`)-1) host_remote,
            tx.trx_state,   tx.trx_operation_state, tx.trx_rows_locked, tx.trx_lock_structs,    tx.trx_started,
            timestampdiff(SECOND, tx.trx_started, now()) duration,
            lo.lock_mode, lo.lock_type, lo.lock_table, lo.lock_index, lo.lock_data, tx.trx_query,
            NULL as blocking_trx_id
        FROM
            information_schema.innodb_trx tx
            INNER JOIN information_schema.innodb_lock_waits lw ON lw.blocking_trx_id = tx.trx_id
            INNER JOIN information_schema.innodb_locks lo ON lo.lock_id = lw.blocking_lock_id
            INNER JOIN information_schema.`PROCESSLIST` p ON p.id = tx.trx_mysql_thread_id
        UNION ALL
        SELECT
            tx.trx_id, 'Blockee' role, p.id thread_id, p.`USER` dbuser,
            LEFT(p.`HOST`, locate(':', p.`HOST`)-1) host_remote,
            tx.trx_state, tx.trx_operation_state, tx.trx_rows_locked, tx.trx_lock_structs, tx.trx_started,
            timestampdiff(SECOND, tx.trx_started, now()) duration,
            lo.lock_mode, lo.lock_type, lo.lock_table, lo.lock_index, lo.lock_data, tx.trx_query,
            lw.blocking_trx_id
        FROM
            information_schema.innodb_trx tx 
            INNER JOIN information_schema.innodb_lock_waits lw ON lw.requesting_trx_id = tx.trx_id
            INNER JOIN information_schema.innodb_locks lo ON lo.lock_id = lw.requested_lock_id 
            INNER JOIN information_schema.`PROCESSLIST` p ON p.id = tx.trx_mysql_thread_id
    """
    try:
        cur = conn.cursor()

        snapshot_file = work_dir+"snapshot_" + threadName + ".txt"
        fo = open(snapshot_file, "a")
        fo.write("\n\n######################################################\n")
        fo.write("##############  " + time.asctime() + "  ##############\n")
        fo.write("######################################################\n")

        logger.debug("Get 'show full processlist' to: %s", snapshot_file)
        fo.write("\n######## show full processlist : ########\n")
        cur.execute(str_fulllist)
        rs_1 = cur.fetchall()
        for row in rs_1:
            fo.write("[[ " + ",\t".join(map(str, row)) + " ]]\n")

        logger.debug("Get 'innodb_lock_waits' to: %s", snapshot_file)
        fo.write("\n\n######## innodb_lock_waits : ########\n")
        fo.write("trx_id, role, thread_id, dbuser, host_remote, trx_state, trx_operation_state, trx_rows_locked, trx_lock_structs, "
                 "trx_started, duration, lock_mode, lock_type, lock_table, lock_index, lock_data, trx_query, blocking_trx_id\n")
        cur.execute(str_trx_lockwait)
        rs_0 = cur.fetchall()
        for row in rs_0:
            fo.write("[[ " + ",\t".join(map(str, row)) + " ]]\n")

        logger.debug("Get 'show engine innodb status' to: %s", snapshot_file)
        fo.write("\n\n######## show engine innodb status : ########\n")
        cur.execute(str_status)
        rs_2 = cur.fetchone()
        fo.write(rs_2[2])

        fo.close()
        snapshot_file_html = work_dir+"snapshot_" + threadName + ".html"
        snapshot_html = snapshot_report.write_mail_content_html(snapshot_file_html, rs_0, rs_1, rs_2[2].replace('\n', '<br/>'))
        return snapshot_html  # filename
    except pymysql.err.MySQLError  as  e:
        logger.critical('Error %d: %s', e.args[0], e.args[1])
    finally:
        cur.close()
        fo.close()

def output_db():
    pass


def kill_threads(threads_tokill, db_conns, db_id, db_commconfig):
    # 没有需要被 kill 的会话
    if len(threads_tokill) == 0:
        logger.debug("no threads need to be kill")
        return 0

    logger.debug("this threads COULD be killed: %s", threads_tokill.__str__())

    process_user = db_commconfig['db_puser']

    # 记录需要被 kill 的 thread_id,主要用于判断是否重复发邮件
    for u, t_id in threads_tokill.items():
        kill_str = ";  ".join("kill %d" % t for t in t_id)
        thread_ids = set(t_id)

        # 明确设置dry_run=0才真正kill
        if db_commconfig['dry_run'] == '0':
            try:
                snapshot_html = get_more_info(db_conns[process_user], db_id)
                logger.info("(%s) run in dry_run=0 mode , do really kill, but the status snapshot is taken", u)
                try:
                    cur = db_conns[u].cursor()
                    cur.execute(kill_str)
                    sendemail(db_id, ' (' + u + ') KILLED', snapshot_html)
                except Exception as e:
                    logger.error("connection:%s not exists", u)
                    sendemail(db_id, ' (' + u + ') Not KILLED,connection not exists ', snapshot_html)

                logger.warn("(%s) kill-command has been executed : %s", u, kill_str)
            except pymysql.err.MySQLError as  e:
                logger.critical('Error %d: %s', e.args[0], e.args[1])
                cur.close()

        else:
            # dry_run模式下可能会反复或者同样需被kill的thread
            logger.info("(%s) run in dry_run=1 mode , do not kill, but take status snapshot the first time", u)

            # 前后两次 threads_tokill里面有共同的id，则不发送邮件
            if thread_ids and not (THREAD_DATA.THREADS_TOKILL.get(u,set()) & thread_ids):
                snapshot_html = get_more_info(db_conns[process_user], db_id)
                sendemail(db_id, ' (' + u + ') NOT KILLED', snapshot_html)

        # store last threads(kill or not kill)
        THREAD_DATA.THREADS_TOKILL[u] = thread_ids

# 邮件通知模块
def sendemail(db_id, dry_run, filename='',title='slow_query'):
    MAIL_CONFIG = get_setttings('mail_config')
    mail_receiver = MAIL_CONFIG['mail_receiver'].split(";")
    mailenv = MAIL_CONFIG['env']

    if mail_receiver == "":
        logger.info("do not send email")
        return

    mail_host = MAIL_CONFIG['mail_host']
    mail_user = MAIL_CONFIG['mail_user']
    mail_pass = MAIL_CONFIG['mail_pass']

    message = MIMEMultipart()

    message['From'] = Header('DBA', 'utf-8')
    message['To'] = Header('devops', 'utf-8')
    if title=='act_session':
        kill_opt = get_setttings("id_" + db_id)
        subject = '(%s) %s active session has been take snapshot' % (mailenv, db_id)
        message.attach(MIMEText('db有超过阀值的活动会话，阀值为:<strong>' + kill_opt['db_act_sess'] + '</strong> <br/>', 'html', 'utf-8'))
    else:
        subject = '(%s) %s slow query has been take snapshot' % (mailenv, db_id)
        message.attach(MIMEText('db有慢查询, threads <strong>' + dry_run + '</strong> <br/>', 'html', 'utf-8'))
    message['Subject'] = Header(subject, 'utf-8')

    message.attach(MIMEText('<br/>You can find more info(snapshot) in the attachment : <strong> ' +
                            filename + ' </strong> processlist:<br/><br/>', 'html', 'utf-8'))

    with open(work_dir+'processlist_'+db_id+'.txt', 'r')as f:
    # with open(filename, 'rb')as f:
        filecontent = f.readlines()
    att1 = MIMEText("<br/>".join(filecontent), 'html', 'utf-8')
    att2 = MIMEText(open(filename, 'rb').read(), 'base64', 'utf-8')
    att2["Content-Type"] = 'application/octet-stream'
    att2["Content-Disposition"] = 'attachment; filename=%s' % filename
    message.attach(att1)
    message.attach(att2)

    try:
        print ("entry mail process:%s"%title)
        smtpObj = smtplib.SMTP_SSL(host=mail_host, port=465, timeout=30)
        smtpObj.ehlo()
        print ("email begin login...:%s"%title)
        smtpObj.login(mail_user, mail_pass)
        print ("email login OK! begin send mail...:%s"%title)
        smtpObj.sendmail(mail_user, mail_receiver, message.as_string())

        logger.info("Email sending succeed:%s"%title)
    except smtplib.SMTPException as  err:
        logger.critical("Error email content:%s ::%s"%(err,title))
        logger.critical("Error: 发送邮件失败(%s, %s)", err.args[0], err.args[1].__str__())
    except Exception as a:
        logger.error("send mail fail %s",a)
    finally:
        smtpObj.quit()

#检查活动的连接数
def check_act_session(conn,kill_opt,db_id):
    logger.debug("begin check active session  from  information_schema.processlist")
    tim=None
    try:
        cur=conn.cursor()
        sqlstr='select count(*) from information_schema.PROCESSLIST where info is not null;'
        cur.execute(sqlstr)
        max_cnt=int(kill_opt['db_act_sess'])
        for i in cur.fetchall():
            if i[0]>=max_cnt:
                snapshot_html=get_more_info(conn,db_id)
                get_active_session(conn, db_id)
                sendemail(db_id, ' NOT KILLED', snapshot_html,'act_session')
                tim=datetime.datetime.now()
            else:
                print("active session cnt:%d,config max cnt is:%d"%(i[0],max_cnt))

    except Exception as e:
        logger.critical("Get active session processlist connection error:%s",e)
    finally:
        cur.close()
    return tim




# for db_instance one python thread: main function
def my_slowquery_kill(db_instance):
    db_id = db_instance[0]
    db_host, db_port = db_instance[1].replace(' ', '').split(':')
    print ("db_id:, db_host:, db_port:" + db_id+db_host+db_port)

    db_commconfig = get_setttings("db_commconfig")



    # 登录db认证信息
    #db_users = json.loads(db_commconfig["db_auth"])
    db_users = settings.DB_AUTH
    print ("db_users::",db_users)

    # 每个db实例的多个用户维持各自的连接
    db_conns = {}

    # db连接密码解密
    pc = prpcryptec.PrpCrypt(KEY_DB_AUTH)
    for user in  db_users:
        db_user,db_pass=user.split(':')
        print ("--gaott2:db_user:",db_user)
        print ("--gaott2:db_pass:",db_pass)
        try:
            try:
                dbpass_de = pc.decrypt(db_pass)
            except Exception as e:
                logger.error("decrypt fail for:%s-->%s", db_user, e)
            try:
                conn = pymysql.connect(db_host,db_user,dbpass_de,'', int(db_port))
                db_conns[db_user] = conn
                logger.debug("connection is created: %s:%s  %s", db_host, db_port, db_user)
            except pymysql.err.MySQLError as a :
                logger.error('conn error user:%s in %s',db_user,db_id)
                if a.args[0] ==1045:
                    logger.error('conn error user not exist or password Is not correct:%s', a.args[1])
                    logger.error('User %s may not exist or password Is not correct in DB %s . Skip it for you.', db_user, db_host)
                    continue
                if a.args[0] ==2003:
                    logger.error('conn error in  first time :target mysql server is wrong :%s', a.args[1])
                    return

        except Exception as  e:
            traceback.print_exc()
            logger.error('Error find in conn: %s',e)

    if db_conns.__len__()==0:
        logger.error('Not exists any usefull connet for:%s', db_id)
        return





    kill_count = 0
    run_max_count_last = 0
    check_ping_wait = 0
    tims = None
    while True:
        db_commconfig = get_setttings("db_commconfig")
        # 获取具体的db_instance 选项kill
        kill_opt = get_setttings("id_" + db_id)

        # 查看processlist连接的作为心跳
        # 如果数据库端 kill掉这个用户的连接，该实例检查则异常退出
        print ("gaott5:",db_commconfig)

        if (db_commconfig['run_time_window'][0] < datetime.datetime.now().strftime("%H:%M") < db_commconfig['run_time_window'][1]) or len(db_commconfig['run_time_window']) == 0:
            run_max_count = int(db_commconfig['run_max_count'])
            if run_max_count != run_max_count_last:
                logger.info("you've changed run_max_count, set a clean start")
                kill_count = 0
                THREAD_DATA.THREADS_TOKILL = {}

                if run_max_count == 999:
                    logger.warn("you've set run_max_count=999 , always check processlist")
                    # kill_count = 0
                if run_max_count == 0:
                    logger.info("you've set run_max_count=0 , stop check processlist & keep user conn alive")
                    run_max_count_last = run_max_count

            if run_max_count == 999:
                kill_count = 0

            if kill_count < run_max_count:
                #开始处理判断mysql线程
                try:
                    threads_tokill = get_processlist_kthreads(db_conns[db_commconfig['db_puser']], kill_opt, db_id)
                except  Exception as   e:
                    logger.error("db_puser  not connect to db  exit.......")
                    return

                kill_threads(threads_tokill, db_conns, db_id, db_commconfig)

                kill_count += 1
                run_max_count_last = run_max_count
                #begin check actie session
                interval=int(kill_opt['db_act_check_interval'])
                if tims ==None or (int((datetime.datetime.now()-tims).seconds)>=interval):
                    tims=check_act_session(conn, kill_opt, db_id)
                else:
                    print("last check time is:%s,interval:%d,next check after %d seconds." %(tims,interval,(interval-int((datetime.datetime.now()-tims).seconds))))
                    logger.debug("last check time is:%s,interval:%d,next check after %d seconds." %(tims,interval,(interval-int((datetime.datetime.now()-tims).seconds))))


        else:
            print ("Not running in time window")
            logger.debug("Not running in time window")
            # fix: 处理慢sql在夜间产生，并持续到白天的情况
            THREAD_DATA.THREADS_TOKILL = {}
            kill_count = 0

        time.sleep(settings.CHECK_CONFIG_INTERVAL)
        # 维持其它用户连接的心跳，即使被kill也会被拉起
        if check_ping_wait == settings.CHECK_PING_MULTI:
            for dc_user in db_conns:
                try:
                    logger.info("(%s) MySQL try ping to keep session alive", dc_user)
                    db_conns[dc_user].ping()
                except Exception as  e:
                    logger.critical('Error in  mysql ping :%s',e)
                    db_conns[dc_user] = db_reconnect(dc_user, db_id)

            check_ping_wait = 0
        else:
            check_ping_wait += 1
            kill_opt = get_setttings("id_" + db_id)


# use multi-thread
class myThread(threading.Thread):
    def __init__(self, threadID, db_instance):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = db_instance[0]

    def run(self):
        logger.info("Starting kill query Thread: %s", self.name)
        #THREAD_DATA.MAIL_SEND_TIMES = 0
        THREAD_DATA.THREADS_TOKILL = {}

        my_slowquery_kill(db_instance)
        logger.info("Exiting Thread: %s", self.name)

if __name__ == '__main__':
    db_instances = get_setttings("db_info")

    # start keep-session-kill threads for every user and db_instance
    for db_instance in db_instances.items():
        # threadName like dbnqqame_user
        thread_to_killquery = myThread(100, db_instance)
        thread_to_killquery.start()
        time.sleep(1)
