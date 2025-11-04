"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/4 13:26
# @Author  : PcLiu
# @FileName: agent_proxy.py
===========================

"""

from fake_useragent import UserAgent

proxies = {
    "http": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
    "https": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823"
}

user_agent = UserAgent(os=["Windows", "Linux", "Ubuntu", "Mac OS X"])

headers = {
    "User-Agent": user_agent.random,
    "Connection": "close",
    "Accept": "text/html,application/xhtml+xml,application/xml",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}




