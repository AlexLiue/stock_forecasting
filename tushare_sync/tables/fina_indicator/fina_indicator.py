"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2022/9/10 06:40
# @Author  : PcLiu
# @FileName: fina_indicator.py
===========================

沪深股票-财务数据-财务指标数据
接口：fina_indicator，可以通过数据工具调试和查看数据。
描述：获取上市公司财务指标数据，为避免服务器压力，现阶段每次请求最多返回60条记录，可通过设置日期多次请求获取更多数据。
权限：用户需要至少800积分才可以调取，具体请参阅积分获取办法
tushare 接口说明：https://tushare.pro/document/2?doc_id=79
"""

import os
import datetime

from tushare_sync.utils.utils import exec_create_table_script, query_last_sync_date, max_date, get_cfg, exec_sync_with_ts_code

begin_date = "20040101"
limit = 3000
interval = 0.3


def exec_sync(start_date, end_date):
    global limit
    global interval
    exec_sync_with_ts_code(
        table_name='fina_indicator',
        api_name='fina_indicator',
        fields=[
            "ts_code",
            "ann_date",
            "end_date",
            "eps",
            "dt_eps",
            "total_revenue_ps",
            "revenue_ps",
            "capital_rese_ps",
            "surplus_rese_ps",
            "undist_profit_ps",
            "extra_item",
            "profit_dedt",
            "gross_margin",
            "current_ratio",
            "quick_ratio",
            "cash_ratio",
            "invturn_days",
            "arturn_days",
            "inv_turn",
            "ar_turn",
            "ca_turn",
            "fa_turn",
            "assets_turn",
            "op_income",
            "valuechange_income",
            "interst_income",
            "daa",
            "ebit",
            "ebitda",
            "fcff",
            "fcfe",
            "current_exint",
            "noncurrent_exint",
            "interestdebt",
            "netdebt",
            "tangible_asset",
            "working_capital",
            "networking_capital",
            "invest_capital",
            "retained_earnings",
            "diluted2_eps",
            "bps",
            "ocfps",
            "retainedps",
            "cfps",
            "ebit_ps",
            "fcff_ps",
            "fcfe_ps",
            "netprofit_margin",
            "grossprofit_margin",
            "cogs_of_sales",
            "expense_of_sales",
            "profit_to_gr",
            "saleexp_to_gr",
            "adminexp_of_gr",
            "finaexp_of_gr",
            "impai_ttm",
            "gc_of_gr",
            "op_of_gr",
            "ebit_of_gr",
            "roe",
            "roe_waa",
            "roe_dt",
            "roa",
            "npta",
            "roic",
            "roe_yearly",
            "roa2_yearly",
            "roe_avg",
            "opincome_of_ebt",
            "investincome_of_ebt",
            "n_op_profit_of_ebt",
            "tax_to_ebt",
            "dtprofit_to_profit",
            "salescash_to_or",
            "ocf_to_or",
            "ocf_to_opincome",
            "capitalized_to_da",
            "debt_to_assets",
            "assets_to_eqt",
            "dp_assets_to_eqt",
            "ca_to_assets",
            "nca_to_assets",
            "tbassets_to_totalassets",
            "int_to_talcap",
            "eqt_to_talcapital",
            "currentdebt_to_debt",
            "longdeb_to_debt",
            "ocf_to_shortdebt",
            "debt_to_eqt",
            "eqt_to_debt",
            "eqt_to_interestdebt",
            "tangibleasset_to_debt",
            "tangasset_to_intdebt",
            "tangibleasset_to_netdebt",
            "ocf_to_debt",
            "ocf_to_interestdebt",
            "ocf_to_netdebt",
            "ebit_to_interest",
            "longdebt_to_workingcapital",
            "ebitda_to_debt",
            "turn_days",
            "roa_yearly",
            "roa_dp",
            "fixed_assets",
            "profit_prefin_exp",
            "non_op_profit",
            "op_to_ebt",
            "nop_to_ebt",
            "ocf_to_profit",
            "cash_to_liqdebt",
            "cash_to_liqdebt_withinterest",
            "op_to_liqdebt",
            "op_to_debt",
            "roic_yearly",
            "total_fa_trun",
            "profit_to_op",
            "q_opincome",
            "q_investincome",
            "q_dtprofit",
            "q_eps",
            "q_netprofit_margin",
            "q_gsprofit_margin",
            "q_exp_to_sales",
            "q_profit_to_gr",
            "q_saleexp_to_gr",
            "q_adminexp_to_gr",
            "q_finaexp_to_gr",
            "q_impair_to_gr_ttm",
            "q_gc_to_gr",
            "q_op_to_gr",
            "q_roe",
            "q_dt_roe",
            "q_npta",
            "q_opincome_to_ebt",
            "q_investincome_to_ebt",
            "q_dtprofit_to_profit",
            "q_salescash_to_or",
            "q_ocf_to_sales",
            "q_ocf_to_or",
            "basic_eps_yoy",
            "dt_eps_yoy",
            "cfps_yoy",
            "op_yoy",
            "ebt_yoy",
            "netprofit_yoy",
            "dt_netprofit_yoy",
            "ocf_yoy",
            "roe_yoy",
            "bps_yoy",
            "assets_yoy",
            "eqt_yoy",
            "tr_yoy",
            "or_yoy",
            "q_gr_yoy",
            "q_gr_qoq",
            "q_sales_yoy",
            "q_sales_qoq",
            "q_op_yoy",
            "q_op_qoq",
            "q_profit_yoy",
            "q_profit_qoq",
            "q_netprofit_yoy",
            "q_netprofit_qoq",
            "equity_yoy",
            "rd_exp",
            "update_flag"
        ],
        date_column='ann_date',
        start_date=start_date,
        end_date=end_date,
        date_step=365,
        limit=3000,
        interval=0.2,
        ts_code_limit=1000
    )


# 全量初始化表数据
def sync(drop_exist):
    # 创建表
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, drop_exist)

    # 查询历史最大同步日期
    global begin_date
    cfg = get_cfg()
    date_query_sql = "select max(ann_date) date from %s.fina_indicator" % cfg['mysql']['database']
    last_date = query_last_sync_date(date_query_sql)
    start_date = max_date(last_date, begin_date)
    end_date = str(datetime.datetime.now().strftime('%Y%m%d'))

    exec_sync(start_date, end_date)


if __name__ == '__main__':
    sync(True)
