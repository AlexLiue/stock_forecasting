"""
配置代理
"""

from akshare.utils.context import AkshareConfig

from akshare_sync.util.tools import get_cfg

cfg = get_cfg()

""" 创建代理字典 """
proxies = {"http": cfg["proxy"]["http"], "https": cfg["proxy"]["https"]}
""" 创建代理字典 """
print(f"Exec Set Proxy[{proxies}]")
AkshareConfig.set_proxies(proxies)
