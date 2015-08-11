#coding=utf8
__author__ = 'Administrator'

import MySQLdb
import datetime
import xlwt
import copy
import time

shorts = {'yzzy':'宇宙知音', 'fjdx':"福建电信", 'gzyd':'广州移动',
                  'dgdx':'东莞电信', 'yw':'运维(营销)', 'gzdx':'广州电信', 'jsyd':'江苏移动'}
channels = {10:['yw', [1]], 11:['gzdx', [2]], 12:['gzyd', [0]], 13:['yzzy', [2]], 14:['yzzy', [0, 2]],
            15:['yzzy', [0]], 16:['fjdx', [0, 1, 2]], 17:['dgdx', [2]], 18:['jsyd', [0, 2]]}
patterns ={
            0:"'^(86){0,1}((13[4-9])|(15([0-2]|[7-9]))|(18([2-4]|[78]))|(147)|(178))'",
            1:"'^(86){0,1}((13[0-2])|(15[56])|(18[56])|(145))'",
            2:"'^(86){0,1}((133)|(153)|(18[019])|(177))'"
            }

def _withinDayCond(field, date):
    return field+" > str_to_date('"+date+" 00:00:00','%Y-%m-%d %H:%i:%s') and "+field+" < str_to_date('"+date+" 23:59:59','%Y-%m-%d %H:%i:%s')"

def _withinDayCondPeriod(field, fromD, toD):
    return field+" > str_to_date('"+fromD+" 00:00:00','%Y-%m-%d %H:%i:%s') and "+field+" < str_to_date('"+toD+" 23:59:59','%Y-%m-%d %H:%i:%s')"

def gather(cursor, database, date, proxyData):
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

def gatherDays(cursor, database, proxyData, toDate, fromDate=''):
    #proxyData={'fjdx':{0: 1000, 2:100}}
    if fromDate == '':
        t = time.strptime(toDate, '%Y-%m-%d')
        y,m = t[0:2]
        fromDate = str(datetime.date(y,m, 1))
    sql = "select distinct channel  from "+ database +" where "+_withinDayCondPeriod("reqTime", fromDate, toDate)+";"
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
            sql = "select count(*)  from "+database+" where channel="+str(row[0])+" and "+_withinDayCondPeriod("reqTime", fromDate, toDate)+" and resCode="+rightCode+";"
            cursor.execute(sql)
            ispNum[isps[0]] = cursor.fetchone()[0]
            proxyData[channels[row[0]][0]] = ispNum
        else:
            ispNums = {}
            for i in isps:
                sql = "select count(*)  from "+database+" where channel="+str(row[0])+" and "+_withinDayCondPeriod("reqTime", fromDate, toDate)+" and resCode="+rightCode+" and mobile regexp "+patterns[i]+";"
                cursor.execute(sql)
                ispNums[i] = cursor.fetchone()[0]
            proxyData[channels[row[0]][0]] = ispNums


def compute(proxyData):
    #proxySums {'fjdx': 3, 'yzzy': 2}
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
    total = 0
    for value in ispSums.values():
        total += value
    return proxySums, ispSums, total

def _tProxySums(proxyData):
    #proxySums
    # {
    # 'fjdx':
    #   {
    #   tc:{sum:10, sent:6, nSent:4, fee:0.6},
    #   nTc:{sent:2, unitPrize:0.035, fee:0.07},
    #   0:{sent:6},
    #   1:{sent:0},
    #   2:{sent:3},
    #   total:{sent:12, fee:0.13
    #   }
    # }
    # tProxyData = {'fjdx':{0:6, 2:3}, 'yzzy':{1:8}, 'yw':{1:4}}
    charges = {
        'gzyd':[[1000000, 50000], 0.05],
        'fjdx':[[0, 0], 0.036],
        'yzzy':[[0, 0], 0.0399],
        'yw':[[0, 0], 0.04],
        'gzdx':[[250000, 14000], 0.06],
        'jsyd':[[0, 0], 0.035]
        }
    proxySums = {}
    for key, value in charges.items():
        tc = {}
        tc['sum'] = value[0][0]
        if proxyData.get(key):
            tSent = sum(proxyData[key].values())
        else:
            tSent = 0
        if tSent <= tc['sum']:
            tc['sent'] = tSent
        else:
            tc['sent'] = tc['sum']
        tc['nSent'] = tc['sum'] - tc['sent']
        tc['fee'] = charges[key][0][1]
        proxySums[key] = {}
        proxySums[key]['tc'] = tc

        nTc = {}
        if tSent > tc['sum']:
            nTc['sent'] = tSent - tc['sum']
        else:
            nTc['sent'] = 0
        nTc['unitPrize'] = charges[key][1]
        nTc['fee'] = nTc['sent'] * nTc['unitPrize']
        proxySums[key]['nTc'] = nTc

        for i in range(3):
            proxySums[key][i] = {}
            proxySums[key][i]['sent'] = 0
        if proxyData.get(key):
            for key1, value1 in proxyData[key].items():
                proxySums[key][key1]['sent'] += value1

        total = {}
        total['sent'] = proxySums[key]['tc']['sent'] + proxySums[key]['nTc']['sent']
        total['fee'] = proxySums[key]['tc']['fee'] + proxySums[key]['nTc']['fee']
        proxySums[key]['total'] = total
    return proxySums

def tCompute(proxyData):
    proxySums = _tProxySums(proxyData)
    #just copy the data structure
    tSums = copy.deepcopy(proxySums[tuple(proxySums.keys())[0]])
    for key, value in proxySums.items():
        for key1, value1 in value.items():
            for key2, value2 in value1.items():
                tSums[key1][key2] = 0
    for key, value in proxySums.items():
        for key1, value1 in value.items():
            for key2, value2 in value1.items():
                tSums[key1][key2] += value2
    return proxySums, tSums

def tPrint(table, style, tProxySums, tSums):
    texts = {
        'tc':['sum', 'sent', 'nSent', 'fee'],
        'nTc':['sent', 'unitPrize', 'fee'],
        0:['sent'],
        1:['sent'],
        2:['sent'],
        'total':['sent', 'fee']
        }
    seqs = ['tc', 'nTc', 0, 1, 2, 'total']
    gaps = [1, 1, 0, 0, 1]
    chinese = {'sent':'已发', 'nSent':'未发', 'sum':'总量', 'fee':'费用', 'unitPrize':'单价',
               'tc':'套餐', 'nTc':'套餐外', 0:'移动', 1:'联通', 2:'电信', 'total':'合计'}
    row = 1
    for i in range(len(seqs)):
        table.write_merge(row, row+len(texts[seqs[i]])-1, 0, 0, chinese[seqs[i]], style)
        seqs1 = texts[seqs[i]]
        for j in range(len(seqs1)):
            table.write(row+j, 1, chinese[seqs1[j]], style)
        if i < len(seqs)-1:
            row += len(texts[seqs[i]]) + gaps[i]
    #proxySums
    # {
    # 'fjdx':
    #   {
    #   tc:{sum:10, sent:6, nSent:4, fee:0.6},
    #   nTc:{sent:2, unitPrize:0.035, fee:0.07},
    #   0:{sent:6},
    #   1:{sent:0},
    #   2:{sent:3},
    #   total:{sent:12, fee:0.13
    #   }
    # }
    col = 2
    for key, value in tProxySums.items():
        row = 1
        table.write(0, col, shorts[key], style)
        tProxySum = tProxySums[key]
        for i in range(len(seqs)):
            seqs1 = texts[seqs[i]]
            for j in range(len(seqs1)):
                table.write(row+j, col, tProxySum[seqs[i]][seqs1[j]], style)
            if i < len(seqs)-1:
                row += len(texts[seqs[i]]) + gaps[i]
        col += 1

        table.write(0, col, '合计', style)
        row = 1
        for i in range(len(seqs)):
            seqs1 = texts[seqs[i]]
            for j in range(len(seqs1)):
                if seqs1[j] != 'unitPrize':
                    table.write(row+j, col, tSums[seqs[i]][seqs1[j]], style)
            if i < len(seqs)-1:
                row += len(texts[seqs[i]]) + gaps[i]


def printXls(table, style, proxyData, proxySums, ispSums, total):
    cols = ['移动', '联通', '电信', '合计']
    row = 1
    #table.write_merge(0, 3, 1, 2, 'a', style)
    #proxyData = {'fjdx':{0: 3, 2:1}, 'yzzy':{1:2}}
    #proxySums {'fjdx': 3, 'yzzy': 2}
    #ispSums = {0:0, 1:0, 2:0}
    for col in cols:
        table.write(row, 0, col, style)
        row += 1
    col = 1
    for key, value in proxyData.items():
        table.write(0, col, shorts[key], style)
        for key1, value1 in value.items():
            table.write(key1+1, col, value1, style)
        table.write(4, col, proxySums[key], style)
        col += 1
    table.write(0, col, '合计', style)
    for key, value in ispSums.items():
        table.write(key+1, col, value, style)
    table.write(4, col, total, style)




if __name__ == '__main__':
    startT = datetime.datetime.now()
    proxyData = {}
    tProxyData = {}
    date = str(datetime.date.today() + datetime.timedelta(days =-2))
    file = xlwt.Workbook(encoding='utf-8')
    style = xlwt.easyxf('align: wrap on, vert centre, horiz center')
    table = file.add_sheet('短信日通报量', cell_overwrite_ok=True)
    table1 = file.add_sheet('截止'+str(int(str(date)[8:]))+"日累计使用量", cell_overwrite_ok=True)
    for i in range(255):
        table.col(i).width = 0x0d00 + 7
        table1.col(i).width = 0x0d00 + 7
    dbConn = MySQLdb.connect(host="221.228.209.13", user="mob_DB", passwd="svb7Ml8+Oc4", db="mobcall", port=6301, charset="utf8")
    cursor = dbConn.cursor()
    gather(cursor, 'sms_status_report', date, proxyData)
    gather(cursor, 'sms_report_char', date, proxyData)
    #proxyData = {'fjdx':{0: 3, 2:1}, 'yzzy':{1:2}}
    proxySums, ispSums, total = compute(proxyData)
    printXls(table, style, proxyData, proxySums, ispSums, total)

    gatherDays(cursor, 'sms_status_report', tProxyData, date)
    gatherDays(cursor, 'sms_report_char', tProxyData, date)
    #tProxyData = {'fjdx':{0:6, 2:3}, 'yzzy':{1:12}, 'yw':{1:4}}
    tProxySums, tSums = tCompute(tProxyData)
    tPrint(table1, style, tProxySums, tSums)

    file.save(r'C:\\Users\\Administrator\\Desktop\\smsDaily\\' + str(date) + '.xls')
    dbConn.commit()
    cursor.close()
    dbConn.close()
    endT = datetime.datetime.now()
    print (endT-startT)


