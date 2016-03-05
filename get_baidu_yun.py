from gevent import monkey
monkey.patch_all()
import gevent
from gevent.pool import Pool
import os
import sys
import datetime
import StringIO
import gzip
import urllib2
import time
import pymongo
import json
import random

class Error(Exception):
    pass

MONGODB = pymongo.MongoClient('127.0.0.1', 27017)
MONGODB_FANS = MONGODB.baidu.yun
MONGODB_SHARE = MONGODB.baidu.share

ALL_ERROR = Exception

HTTP_ERROR_MAX = 50
HTTP_TIME_OUT = 30
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/31.0.1650.63 Safari/537.36'
)
ACCEPT_ENCODING = 'gzip,deflate,sdch'

def getUrl(url, use_gzip=True, timeout=HTTP_TIME_OUT, proxy_info=None):
    if not url:
        return '', None

    if proxy_info:
        proxy_support = urllib2.ProxyHandler({"http" : "http://%s" % proxy_info})
        opener = urllib2.build_opener(proxy_support)
        urllib2.install_opener(opener)
        getUrl.proxy_info = proxy_info

    req = urllib2.Request(url)
    if use_gzip:
        req.add_header('Accept-Encoding', ACCEPT_ENCODING)
    req.add_header('User-Agent', USER_AGENT)
    res = urllib2.urlopen(req, timeout=timeout)

    headers, data = res.headers, res.read()
    if headers.getheader('Content-Encoding', default='').lower()=='gzip':
        try:
            data = gzip.GzipFile(fileobj=StringIO.StringIO(data)).read()
        except KeyboardInterrupt as ex:
            raise ex
        except ALL_ERROR:
            if use_gzip:
                return getUrl(url, use_gzip=False, timeout=timeout, proxy_info=proxy_info)
    return data, headers

def get_proxy_info():
    proxy_list = getattr(get_proxy_info, 'proxy_list', None)
    if proxy_list is None and os.path.isfile(os.path.join(os.getcwd(), 'ip.txt')):
        with open('ip.txt', 'r') as rf:
            proxy_list = [i.replace('\n', '') for i in rf if ':' in i]
        get_proxy_info.proxy_list = proxy_list
    if proxy_list:
        return random.choice(proxy_list)
    return None

def get_data(uk_in, url, ret_func, isok_func, error_count_max=HTTP_ERROR_MAX):
    ret = {}

    error_count = 0
    proxy_info = None
    while 1:
        if ret is None:
            proxy_info = get_proxy_info()
            #_LOG('%d use proxy_info:<%s>' % (uk_in, proxy_info))
            #_LOG('%s%s' % ('.'*(error_count+1), url))
            error_count += 1
            if error_count>=error_count_max:
                _LOG('failure in:%d<%s>(%s)' % (uk_in, url, proxy_info))
                return {}

        try:
            data, headers = getUrl(url, proxy_info=proxy_info)
            ret = ret_func(data, headers)
        except KeyboardInterrupt as ex1:
            _LOG('KeyboardInterrupt.')
            raise ex1
        except ALL_ERROR as ex2:
            proxy_info = getattr(getUrl, 'proxy_info', '')
            _LOG('ALL_ERROR:%r <%s>' % (ex2.__class__, proxy_info))

        if isok_func(ret):
            return ret
        else:
            ret = None

def get_follow(uk_in):
    ret_func = lambda data, headers: json.loads(data)
    isok_func=lambda ret: isinstance(ret, dict) and 'follow_list' in ret
    url = r'http://pan.baidu.com/pcloud/friend/getfollowlist?query_uk=%d' % (uk_in, )

    index = 0
    limit = '&limit=24&start=%d' % (index)
    ret = get_data(uk_in, url+limit, ret_func=ret_func, isok_func=isok_func)
    uk_dict = {i['follow_uk']:i for i in ret['follow_list']} if isok_func(ret) else {}
    total_count = ret.get('total_count', 0)

    while total_count>24:
        index += 24
        limit = '&limit=24&start=%d' % (index)
        ret = get_data(uk_in, url+limit, ret_func=ret_func, isok_func=isok_func)
        uk_dict.update({i['follow_uk']:i for i in ret['follow_list']} if isok_func(ret) else {})
        total_count -= 24
    return uk_dict

def do_uk_follow(uk_in, num):
    #_LOG('get uk_follow:%d <%d>' % (uk_in, num))
    uk_follow = get_follow(uk_in)
    follow_len = len(uk_follow)
    #_LOG('save uk_follow:%d <%d>' % (uk_in, follow_len))

    error = 0
    for uk, item in uk_follow.items():
        if item['follow_count']==0:
            item['follow_flag'] = 1
        item['done'] = 0 if item['fans_count']>0 else 1
        item['uk'] = uk
        try:
            MONGODB_FANS.insert_one(item)
        except pymongo.errors.PyMongoError as ex:
            error += 1
            #_LOG('MONGODB_FANS.insert_one:%d -> %s' % (uk_in, ex) )

    MONGODB_FANS.update_one({'uk':uk_in}, {'$set': {'follow_flag':1}}, upsert=False)
    _LOG('set follow_flag:%d(%d) <duplicate key:%d(%d)>' % (uk_in, num, error, follow_len))


def get_fans(uk_in):
    ret_func = lambda data, headers: json.loads(data)
    isok_func=lambda ret: isinstance(ret, dict) and 'fans_list' in ret
    url = r'http://pan.baidu.com/pcloud/friend/getfanslist?query_uk=%d' % (uk_in, )

    index = 0
    limit = '&limit=24&start=%d' % (index)
    ret = get_data(uk_in, url+limit, ret_func=ret_func, isok_func=isok_func)
    uk_dict = {i['fans_uk']:i for i in ret.get('fans_list', [])} if isok_func(ret) else {}
    total_count = ret.get('total_count', 0)

    while total_count>24:
        index += 24
        limit = '&limit=24&start=%d' % (index)
        ret = get_data(uk_in, url+limit, ret_func=ret_func, isok_func=isok_func)
        uk_dict.update({i['fans_uk']:i for i in ret.get('fans_list', [])} if isok_func(ret) else {})
        total_count -= 24
    return uk_dict

def do_uk_fans(uk_in, num):
    _LOG('get uk_fans:%d <%d>' % (uk_in, num))
    uk_fans = get_fans(uk_in)
    fans_len = len(uk_fans)
    _LOG('save uk_fans:%d <%d>' % (uk_in, fans_len))

    error = 0
    for uk, item in uk_fans.items():
        item['done'] = 0 if item['fans_count']>0 else 1
        item['uk'] = uk
        item['_id'] = uk
        try:
            MONGODB_FANS.insert_one(item)
        except pymongo.errors.PyMongoError as ex:
            error += 1
            #_LOG('MONGODB_FANS.insert_one:%d -> %s' % (uk_in, ex) )

    MONGODB_FANS.update_one({'uk':uk_in}, {'$set': {'done':1}}, upsert=False)
    _LOG('done:%d(%d) <duplicate key:%d(%d)>' % (uk_in, num, error, fans_len))




def get_share(uk_in):
    ret_func = lambda data, headers: json.loads(data)
    isok_func = lambda ret: isinstance(ret, dict) and 'list' in ret and ret.get('errno', -1)==0
    url = r'http://yun.baidu.com/share/homerecord?uk=%d' % (uk_in, )
    shorturl_dict = {}

    index = 0
    while True:
        index += 1
        limit = '&page=%d&pagelength=60' % (index,)
        ret = get_data(uk_in, url+limit, ret_func=ret_func, isok_func=isok_func)
        uk_share = ret.get('list', [])
        shorturl_dict.update({i['shorturl']:i for i in uk_share})
        if len(uk_share)<60:
            break
    return shorturl_dict


def do_uk_share(uk_in, num):
    _LOG('get uk_share:%d <%d>' % (uk_in, num))

    uk_share = get_share(uk_in)
    share_len = len(uk_share)
    _LOG('save uk_share:%d <%d>' % (uk_in, share_len))

    error = 0
    for _, item in uk_share.items():
        item['_id'] = item.get('shorturl', '')
        item['update_time'] = datetime.datetime.now()
        try:
            MONGODB_SHARE.insert_one(item)
        except pymongo.errors.PyMongoError as ex:
            error += 1
            #_LOG('MONGODB_SHARE.insert_one:%d -> %s' % (uk_in, ex) )

    MONGODB_FANS.update_one({'uk':uk_in}, {'$set': {'done':2}}, upsert=False)
    _LOG('done:%d(%d) <duplicate key:%d(%d)>' % (uk_in, num, error, share_len))


def do_fans(p, spawn_num, min_count):
    ret_iter = MONGODB_FANS.find({'done':0, 'fans_count':{'$gt':min_count}})
    for ret in ret_iter:
        if spawn_num<=1:
            do_uk_fans(ret['uk'], ret['fans_count'])
        else:
            p.spawn(do_uk_fans, ret['uk'], ret['fans_count'])


def do_share(p, spawn_num, min_count):
    ret_iter = MONGODB_FANS.find({'done':1, 'pubshare_count':{'$gt':min_count}})
    for ret in ret_iter:
        if spawn_num<=1:
            do_uk_share(ret['uk'], ret['pubshare_count'])
        else:
            p.spawn(do_uk_share, ret['uk'], ret['pubshare_count'])


def do_follow(p, spawn_num, min_count):
    ret_iter = MONGODB_FANS.find({'follow_flag':None, 'follow_count':{'$gt':min_count}})
    for ret in ret_iter:
        if spawn_num<=1:
            do_uk_follow(ret['uk'], ret['follow_count'])
        else:
            p.spawn(do_uk_follow, ret['uk'], ret['follow_count'])


def main():
    log_file = None
    USE_LOG_FILE = False
    if USE_LOG_FILE:
        log_file = open(os.path.join(os.getcwd(), "%s_%d.log"%(__file__, os.getpid())), 'w')
        _LOG.log_file = log_file
        _LOG('log_file:%s' % (log_file,))

    get_proxy_info()
    proxy_list = getattr(get_proxy_info, 'proxy_list', [])
    _LOG('proxy ip:%d' % (len(proxy_list),))

    spawn_num = 50
    min_count = 10
    try:
        while 1:
            try:
                p = Pool(spawn_num)
                do_follow(p, spawn_num, min_count)
                #do_fans(p, spawn_num, min_count)
                #do_share(p, spawn_num, min_count)
            except pymongo.errors.PyMongoError as ex:
                _LOG('pymongo.errors.PyMongoError:%s' % (ex,))
            finally:
                p.join()
    finally:
        MONGODB.close()
        _LOG('main end.')
        if log_file:
            log_file.close()

def _LOG(msg_in, time_now=True, new_line=True):
    if time_now:
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        msg = '%s => %s' % (time_str, msg_in)
    if getattr(_LOG, 'log_file', None):
        _LOG.log_file.write(msg+'\n')
        _LOG.log_file.flush()

    if new_line:
        print msg
    else:
        print msg,

if __name__ == '__main__':
    main()


