import akshare as ak
import pandas as pd

from akshare_sync.stock_basic_info import stock_basic_info
from akshare_sync.util.tools import get_cfg, get_engine, query_table_is_exist
from akshare_sync.util.tools import get_logger


class GlobalData:
    """
    交易所 |板块  |
    ----+----+
    SZSE|主板  |
    SZSE|创业板 |
    SSE |主板A股|
    SSE |科创板 |
    HKSE|港交所 |
    BSE |北交所 |

    """

    def __init__(self):
        self.value = None
        self._initialized = False

    def __get__(self, instance, owner):
        if not self._initialized:
            self.initialize()
            self._initialized = True
        return self.value

    cfg = get_cfg()
    logger = get_logger("global_data", cfg["sync-logging"]["filename"])
    logger.info("Exec Init Global Shared Data...")

    engine = get_engine()
    table_exist = query_table_is_exist("STOCK_BASIC_INFO")
    if not table_exist:
        stock_basic_info.sync(False)

    query_sql = (
        f'SELECT "证券代码", "证券简称", "交易所" ,"板块"'
        f"FROM STOCK_BASIC_INFO sbi "
        "WHERE \"证券代码\" not like 'ST%%' AND \"证券代码\" not like '*ST%%'"
        f'ORDER BY "证券代码" ASC'
    )
    logger.info(f"Execute SQL [{query_sql}]")
    basic_info = pd.read_sql(query_sql, engine)

    trade_date_a = list(
        ak.tool_trade_date_hist_sina()["trade_date"].apply(
            lambda d: d.strftime("%Y%m%d")
        )
    )
    trade_date_a.sort()

    trade_code_a = basic_info[basic_info["交易所"].isin(["SZSE", "SSE", "BSE"])]

    def initialize(self):
        pass
