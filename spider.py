import redis
import requests
import time
import hashlib
from CONFIG import *
from lxml import etree
from multiprocessing import Pool
from threading import Thread


# 连接本机redis数据库
sql = redis.StrictRedis(host="localhost", port=6379)

def get_html(url, headers):
    """
    获取代理ip网页
    :param url:
    :param headers:
    :return:
    """
    try:
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        response.encoding = "utf-8"
    except requests.HTTPError :
        return None

    return response.text


def parse_html(html):
    """
    解析网页，提取ip，port，http/https
    :param html:
    :return:
    """
    selector = etree.HTML(html)
    roots = selector.xpath('//table[@id="ip_list"]/tr')
    for root in roots:
        proxyData = {}
        try:
            proxyData["ip"] = root.xpath("./td[2]/text()")[0]
            proxyData["port"] = root.xpath("./td[3]/text()")[0]
            proxyData["type"] = root.xpath("./td[6]/text()")[0]
        except IndexError:
            continue

        yield proxyData

def filter_ip(proxyData):
    """
    过滤ip，留下可用ip
    :param proxyData:
    :return:
    """
    # 剔除https类型的
    if proxyData["type"].upper() == "HTTPS":
        return

    del proxyData["type"]
    # 构造requests模块ip代理的参数结构
    proxyData = {
        "http":"http://"+proxyData["ip"]+":"+proxyData["port"]
    }

    try:
        response = requests.get(url="http://www.baidu.com/", proxies=proxyData)
        response.raise_for_status()
    except:
        print(f"{proxyData}不可用")
        return None

    # 若可用，存入redis
    to_redis(proxyData)

def set_threading(item):
    """
    设置线程函数
    :param item:
    :return:
    """
    t = Thread(target=filter_ip, args=(item, ))
    t.start()
    t.join()

def to_redis(value):
    """
    存入redis
    :param value:
    :return:
    """
    md5 = hashlib.md5()
    md5.update(value["http"].encode())
    # field利用md5加密，防重复
    sql.hset("IpPool", md5.hexdigest(), value)

def main():

    for page in range(1, 4):
        html = get_html(URL+str(page), HEADERS)
        print(f"进入第{page}页")
        if not html:
            print("获取代理ip网页失败")
            continue

        # 设置进程池
        pool = Pool(10)
        for item in parse_html(html):
            pool.apply_async(set_threading, (item, ))

        pool.close()
        pool.join()

if __name__ == "__main__":
    atime = time.time()
    main()
    btime = time.time()
    print(int(btime) - int(atime))