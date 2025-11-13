

""" 创建代理字典 """
from akshare.utils.context import AkshareConfig

proxies = {
    "http": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
    "https": "http://415b8ce2027f2686bd2d__cr.cn,hk:1207de794b991714@proxy.cheapproxy.net:823",
}
""" 创建代理字典 """
AkshareConfig.set_proxies(proxies)


import akshare as ak

stock_value_em_df = ak.stock_value_em_by_date(trade_date="20251112")
print(stock_value_em_df)
