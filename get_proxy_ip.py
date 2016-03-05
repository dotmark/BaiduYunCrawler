import os
import sys
import datetime
import StringIO
import gzip
import urllib2
import time
import json
import random
import BeautifulSoup


class Error(Exception):
    pass


ALL_ERROR = Exception

HTTP_ERROR_MAX = 5
HTTP_TIME_OUT = 20
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/31.0.1650.63 Safari/537.36'
)
ACCEPT_ENCODING = 'gzip,deflate,sdch'

def getUrl(url, use_gzip=True, timeout=HTTP_TIME_OUT, proxy_info=None):
    if not url:
        return '', None

    if proxy_info:
        getUrl.proxy_info = proxy_info
        proxy_support = urllib2.ProxyHandler({"http" : "http://%s" % proxy_info})
        opener = urllib2.build_opener(proxy_support)
        urllib2.install_opener(opener)

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
                return getUrl(url, use_gzip=False, timeout=timeout)
    return data, headers

def walk_dir(str_dir, filter_func=None, max_deep=-1):
    ret_dict = {}
    str_dir = str_dir.encode('GBK', 'ignore') if isinstance(str_dir, unicode) else str_dir
    if max_deep==0 or not isinstance(str_dir, str) or not os.path.isdir(str_dir):
        return ret_dict

    current_files = os.listdir(str_dir)
    for file_name in current_files:
        full_name = os.path.join(str_dir, file_name)
        if os.path.isfile(full_name):
            if filter_func is not None:
                if filter_func(full_name):
                    ret_dict[full_name] = file_name
            else:
                ret_dict[full_name] = file_name
        elif os.path.isdir(full_name):
            tmp_dict = walk_dir(full_name, filter_func, max_deep-1)
            ret_dict.update(tmp_dict)

    return ret_dict

def save_html():
    url_tpl = "http://www.proxycn.cn/html_proxy/http-%s.html"
    url_list = [url_tpl % (i,) for i in range(1, 18)]
    for url in url_list:
        data, headers = getUrl(url)
        with open(url.split('/')[-1], 'w') as wf:
            wf.write(data)

def read_html():
    def get_ip(html):
        def get_tag(tag):
            tmp = tag.findAll('td')
            ip = tag.attrs[2][1]
            ip = ip[ip.find('(')+2:ip.find(':')]
            return (ip, tmp[2].text, tmp[3].text, tmp[4].text)

        soup = BeautifulSoup.BeautifulSoup(html)
        tag_list = soup.findAll(name='tr', attrs={'bgcolor':'#fbfbfb'})
        return [get_tag(tag) for tag in tag_list]

    ret_list = []
    file_list = walk_dir('.\\proxy\\', lambda s:s.lower().endswith('.html'), 1).keys()
    for file_str in file_list:
        with open(file_str, 'r') as rf:
            data = rf.read()
        ia = data.find('<tr align="center"><td class="list_title">')
        ib = data.find('</TABLE>', ia)
        ret_list.extend(get_ip(data[ia:ib].decode('gb2312')))
    with open('proxy_ip.txt', 'w') as wf:
        for item in ret_list:
            wf.write('%s:%s@%s#%s\n' % item)
    return ret_list

def main():
    log_file = None
    USE_LOG_FILE = False
    if USE_LOG_FILE:
        log_file = open(os.path.join(os.getcwd(), "%s_%d.log"%(__file__, os.getpid())), 'w')
        _LOG.log_file = log_file
        _LOG('log_file:%s' % (log_file,))

    read_html()

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


