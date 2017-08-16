#!/usr/bin/env python
# -*- coding: gbk -*-


###########################################  
# File Name     : reducer.py
# Author        : liqibo(liqibo@baidu.com)
# Created Time  : 2017/7/10
# Brief         : reducer
###########################################


__revision__ = '0.1'
import sys


def main():
    '''
    statement
    '''
    last_key = ""
    # {url: charge}
    dict_url = {}
    for line in sys.stdin:
        # '\t': uid, url, charge
        line = line.strip()
        ll = map(lambda x:x.strip(), line.split("\t"))
        if len(ll) != 3:
            continue
        uid = ll[0]
        url = ll[1]
        charge = int(ll[2])

        if not last_key:
            last_key = uid
        if last_key != uid:
            for url in dict_url:
                print "%s\t%s\t%d" % (last_key, url, dict_url[url])
            last_key = uid
            dict_url.clear()
        if url not in dict_url:
            dict_url[url] = 0
        dict_url[url] += charge
    # last record
    for url in dict_url:
        print "%s\t%s\t%d" % (last_key, url, dict_url[url])

    return 0



if __name__ == "__main__":
    main()




