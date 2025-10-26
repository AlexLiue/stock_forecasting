import akshare as ak

from akshare_sync.util.tools import get_cfg
from akshare_sync.util.tools import get_logger
class GlobalData:

    cfg = get_cfg()
    logger = get_logger('global_data', cfg['sync-logging']['filename'])
    logger.info("Exec Init Global Shared Data...")

    trade_date_a = list(ak.tool_trade_date_hist_sina()['trade_date'].apply(lambda d: d.strftime('%Y%m%d')))
    trade_code_a = list(ak.stock_info_a_code_name()['code'])
    trade_code_sh_a = list(ak.stock_info_sh_name_code(symbol="主板A股")['证券代码'])
    trade_code_sh_b = list(ak.stock_info_sh_name_code(symbol="主板B股")['证券代码'])
    trade_code_sh_kcb = list(ak.stock_info_sh_name_code(symbol="科创板")['证券代码'])
    trade_code_sz_a = list(ak.stock_info_sz_name_code(symbol="A股列表")['A股代码'])
    trade_code_sz_b = list(ak.stock_info_sz_name_code(symbol="B股列表")['B股代码'])
    trade_code_sz_cdr = list(ak.stock_info_sz_name_code(symbol="CDR列表")['CDR代码'])
    trade_code_bj = list(ak.stock_info_bj_name_code()['证券代码'])

    trade_date_a.sort()
    trade_code_a.sort()
    trade_code_sh_a.sort()
    trade_code_sh_b.sort()
    trade_code_sh_kcb.sort()
    trade_code_sz_a.sort()
    trade_code_sz_b.sort()
    trade_code_sz_cdr.sort()
    trade_code_bj.sort()




