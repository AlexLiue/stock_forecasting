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
from fake_useragent import UserAgent
from requests import exceptions


from akshare_sync.util.tools import get_cfg


