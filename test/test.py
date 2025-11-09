import requests

import akshare as ak

import tushare as ts
from akshare.request import make_request_with_retry_text
from akshare.utils.context import AkshareConfig

from akshare_sync.util.tools import get_cfg


import logging
import urllib3

# 设置urllib3的日志级别为DEBUG，以显示所有级别的日志
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.captureWarnings(True)
log = logging.getLogger("urllib3")
log.setLevel(logging.WARN)

ts.subs()
print(ak.__version__)

cfg = get_cfg()
proxies = {"http": cfg["proxies"]["http"], "https": cfg["proxies"]["https"]}

AkshareConfig.set_proxies(proxies)


stock_sse_summary_df = ak.stock_sse_summary()
print(stock_sse_summary_df)

from akshare_sync.akshare_overwrite.overwrite_function import (
    stock_zh_a_hist,
    request_get,
    request_post,
)


url = "https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"
data = {
    "__EVENTTARGET": "btnSearch",
    "__EVENTARGUMENT": "",
    "__VIEWSTATE": "/wEPDwULLTE1Nzg3NjcwNjdkZM8zzyaV3U9aqcBNqiNQde3z/Csd",
    "__VIEWSTATEGENERATOR": "3B50BBBD",
    "today": "20251106",
    "sortBy": "shareholding",
    "sortDirection": "desc",
    "originalShareholdingDate": "2025/11/05",
    "alertMsg": "",
    "txtShareholdingDate": "2025/11/05",
    "txtStockCode": "01810",
    "txtStockName": "",
    "txtParticipantID": "",
    "txtParticipantName": "",
    "txtSelPartID": "",
}
r = request_post(url, data=data)


print(r)
