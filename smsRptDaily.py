#coding=gbk
__author__ = 'Administrator'

import MySQLdb
import datetime

shorts = {'yzzy':'宇宙知音', 'fjdx':"福建电信", 'gzyd':'广州移动',
                  'dgdx':'东莞电信', 'yw':'运维', 'gzdx':'广州电信', 'jsyd':'江苏移动'}
channels = {10:['yw', [1]], 11:['gzdx', [2]], 12:['gzyd', [0]], 13:['yzzy', [2]], 14:['yzzy', [0, 2]],
            15:['yzzy', [0]], 16:['fjdx', [0, 2]], 17:['dgdx', [2]], 18:['jsyd', [0, 2]]}
date = str(datetime.date.today() + datetime.timedelta(days =-1))
patterns ={
            0:"'^(86){0,1}((13[4-9])|(15([0-2]|[7-9]))|(18([2-4]|[78]))|(147)|(178))'",
            1:"'^(86){0,1}((13[0-2])|(15[56])|(18[56])|(145))'",
            2:"'^(86){0,1}((133)|(153)|(18[019])|(177))'"
            }

def _withinDayCond(field, date):
    return field+" > str_to_date('"+date+" 00:00:00','%Y-%m-%d %H:%i:%s') and "+field+" < str_to_date('"+date+" 23:59:59','%Y-%m-%d %H:%i:%s')"

def gather(cursor, database, proxyData):
    #proxyData={'fjdx':{0: 1000, 2:100}}
    sql = "select distinct channel  from "+ database +" where "+_withinDayCond("reqTime", date)+";"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        isps = channels[row[0]][1]
        rightCode = "'DELIVRD'"
        if row[0] == 16:
            rightCode = '1'
        #proxyData['宇宙知音'] = {0: 88025L, 2:4455L}
        #speed up the sql if 1
        if len(isps) == 1:
            ispNum = {}
            sql = "select count(*)  from "+database+" where channel="+str(row[0])+" and "+_withinDayCond("reqTime", date)+" and resCode="+rightCode+";"
            cursor.execute(sql)
            ispNum[isps[0]] = cursor.fetchone()[0]
            proxyData[channels[row[0]][0]] = ispNum
        else:
            ispNums = {}
            for i in isps:
                sql = "select count(*)  from "+database+" where channel="+str(row[0])+" and "+_withinDayCond("reqTime", date)+" and resCode="+rightCode+" and mobile regexp "+patterns[i]+";"
                cursor.execute(sql)
                ispNums[i] = cursor.fetchone()[0]
            proxyData[channels[row[0]][0]] = ispNums

def compute(proxyData):
    #proxySums
    proxySums = {}
    for key, value in proxyData.items():
        proxySums[key] = 0
        for value1 in value.values():
            proxySums[key] += value1
    #ispSums
    ispSums = {0:0, 1:0, 2:0}
    for value in proxyData.values():
        for key1, value1 in value.items():
            ispSums[key1] += value1
    return proxySums, ispSums


    


def printXls():
    pass



if __name__ == '__main__':
    proxyData = {}
    dbConn = MySQLdb.connect(host="221.228.209.13", user="mob_DB", passwd="svb7Ml8+Oc4", db="mobcall", port=6301, charset="utf8")
    cursor = dbConn.cursor()
    gather(cursor, 'sms_status_report', proxyData)
    gather(cursor, 'sms_report_char', proxyData)
    proxySums, ispSums = compute(proxyData)
    print proxySums, ispSums, proxyData
    dbConn.commit()
    cursor.close()
    dbConn.close()


