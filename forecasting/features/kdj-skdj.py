"""
SKDJ慢速随机指标

N:=9; M:=3;
LOWV:=LLV(LOW,N);
HIGHV:=HHV(HIGH,N);
RSV:=EMA((CLOSE-LOWV)/(HIGHV-LOWV)*100,M);
K:EMA(RSV,M);
D:MA(K,M);

KDJ经典版KDJ
RSV:=(CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100;
K:SMA(RSV,M1,1);
D:SMA(K,M2,1);
J:3*K-2*D;

1.指标>80 时，回档机率大；指标<20 时，反弹机率大；
2.K在20左右向上交叉D时，视为买进信号；
3.K在80左右向下交叉D时，视为卖出信号；
4.SKDJ波动于50左右的任何讯号，其作用不大。
5、K线上穿 D线 -> 买入
6、D线上穿 K线 -> 卖出

SKDJ 背离存在钝化情况
建议 N:=36  M:=5  (默认 N:=9; M:=3)

"""

import numpy as np
import pandas as pd
import plotly.express as px

from tushare.util.formula import SKDJ

from forecasting.utils.utils import load_table

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('display.width', 100000)
np.set_printoptions(threshold=np.inf)


def test():
    ts_code = '000001.SZ'
    start_date = '20230101'
    end_date = '20240501'

    daily = load_table('daily', ts_code=ts_code, start_date=start_date, end_date=end_date)
    daily['trade_date'] = daily['trade_date'].map(lambda date: str(date))

    df = SKDJ(daily, 36, 5)

    df['trade_date'] = daily['trade_date']

    fig = px.line(df, x='trade_date', y=['SKDJ_K', 'SKDJ_D'],
                  labels={'value': '百分比位', 'AA': 'NNN'})
    # 显示图表
    fig.show()


if __name__ == '__main__':
    test()
