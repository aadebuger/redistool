import genpy

import MySQLdb

from urlparse import urlparse
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

import json
import redis
import csv
import sys
import codecs
import sys
#from ttypes import *
import qihuothrift.qihuo.QihuoService

socket = TSocket.TSocket("192.168.1.161", 7911)

transport = TTransport.TBufferedTransport(socket)

r = redis.StrictRedis(host='192.168.1.161',port=6379,db=0)


def redisset(key,f):
    l = r.keys(key)
    for key1 in l:
        print 'key=',key1,'\r\n'
        str1 =torow(key1)
        print 'str1='+str1
        try:
            f.write(str1.decode("utf8"))
            f.write('\n')
        except:
            pass
def torow(key):
    attrs=['username','zhucelaiyuan','email','telnum','cishu','maxtime','mintiem','cishu','onlinesumtime','shouye','huodongshuoming','jiangxiangshezhi','jidongpaihang','zhongjianggonggao','jinpaiqihuo','guanjungushi','gettimu','xiazhucishu','qihuokaicang','qihuopingcang','guabuynum','guasellnum','shouyefenxiang','xiazhufenxiang','kaicangfenxiang','pingcangfenxiang','buygupfenxiang','sellgupfenxiang']
    hash = r.hgetall(key)
    str =''
    print hash
    for attr in attrs:
        value = hash.get(attr)
        if value==None:
                value='0'
        print value+','
        str = str+ value +','
    print '\n'
    return str
def redis2csv(csvfile):
    f = codecs.open(csvfile, "w", "utf-8")
    redisset("tongji*",f)

       
def csv2redishash(csvfile,rediskey,attr):
    spamReader = csv.reader(open(csvfile), delimiter=',', quotechar='|')
    for row in spamReader:
                print 'row',row,'len=',len(row),'0=',row[0]
                r.hmset("%s.%s"%(rediskey,row[0]),{attr:row[1]})
if __name__ == "__main__":
#    redisset("tongji*")
     redis2csv("over.csv")