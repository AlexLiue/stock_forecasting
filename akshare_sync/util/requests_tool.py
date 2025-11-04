"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/4 14:43
# @Author  : PcLiu
# @FileName: requests_tool.py
===========================
"""
import time
import traceback

import requests
from requests import exceptions

from akshare_sync.util.agent_proxy import user_agent, proxies


def request_get(url, params=None, timeout=20):
    max_retry = 5
    cur_retry = 0
    while cur_retry < max_retry:
        try:
            r = requests.get(url, params=params, timeout=timeout, proxies=proxies, headers={
                "User-Agent": user_agent.random,
                "Connection": "close",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
            })

            if r.status_code == 200:
                return r
            break
        except Exception as e:
            print(traceback.format_exc())
            cur_retry += 1
            time.sleep(1)
    if cur_retry == max_retry:
        raise exceptions.RequestException

    return None


