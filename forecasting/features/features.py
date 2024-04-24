"""
特征值计算

"""
from plotly import graph_objs as go

from forecasting.features.SKDJ import GET_SKDJ
from forecasting.utils.utils import load_table, get_cfg, get_mock_connection, get_logger


def get_features(symbol, start_date, end_date):
    """
    特征计算
    :param symbol:  股票代码
    :param start_date: 开始日期
    :param end_date:  结束日期
    :return: 特征值
    """
    cfg = get_cfg()
    db = cfg['mysql']['database']
    logger = get_logger('features', cfg['sync-logging']['filename'])

    engine = get_mock_connection()

    # 加载数据
    daily = load_table(engine=engine, db=db, table='stock_daily_qfq', symbol=symbol,
                       start_date=start_date, end_date=end_date, logger=logger)
    F_SKDJ = GET_SKDJ(daily=daily, N=36, M=5)

    return F_SKDJ

    #
    # daily['avg'] = (daily['amount'] * 10) / daily['vol']
    # skdj =
    # dif_eavg_d = DIF_AVG_E(daily, 2, 5).rename(columns={'DIF_AVG_E': 'DIF_AVG_ED'})
    # dif_eavg_w = DIF_AVG_E(daily, 7, 15).rename(columns={'DIF_AVG_E': 'DIF_AVG_EW'})
    # dif_eavg_m = DIF_AVG_E(daily, 31, 61).rename(columns={'DIF_AVG_E': 'DIF_AVG_EM'})
    # return pd.concat(
    #     [daily[['ts_code', 'trade_date', 'avg']],
    #      skdj,
    #      dif_eavg_d, dif_eavg_w, dif_eavg_m
    #
    #      ],
    #     axis=1)


def plot_skdj_avg(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['trade_date'], y=df['SKDJ_K'], name='SKDJ', xaxis='x', yaxis='y1'))
    fig.add_trace(go.Scatter(x=df['trade_date'], y=df['SKDJ_D'], name='SKDJ', xaxis='x', yaxis='y1'))
    # fig.add_trace(go.Scatter(x=df['trade_date'], y=df['avg'], name='AVG', xaxis='x', yaxis='y2'))
    # fig.add_trace(go.Scatter(x=df['trade_date'], y=df['DIF_AVG_ED'], name='DIF_AVG_ED', xaxis='x', yaxis='y3'))
    # fig.add_trace(go.Scatter(x=df['trade_date'], y=df['DIF_AVG_EW'], name='DIF_AVG_EW', xaxis='x', yaxis='y3'))
    # fig.add_trace(go.Scatter(x=df['trade_date'], y=df['DIF_AVG_EM'], name='DIF_AVG_EM', xaxis='x', yaxis='y3'))
    fig.update_layout(
        title_text=f"{df.at[0, 'ts_code']} N 日特征参数", title_x=0.5,
        margin=dict(l=10, r=10, b=40, t=40),
        xaxis=dict(domain=[0.08, 0.98], showline=True),
        yaxis1=dict(title=dict(text='SKDJ-( K / D 百分位)', standoff=10), titlefont=dict(color="#006400"),
                    tickfont=dict(color="#006400"),
                    side="left", position=0, showline=True, linecolor='#006400'),
        # yaxis2=dict(title="成交均价", titlefont=dict(color='#000000'), tickfont=dict(color='#000000'),
        #             overlaying='y', side="right", position=1, showline=True, linecolor='#000000'),
        # yaxis3=dict(title=dict(text='DIF_AVG', standoff=0), titlefont=dict(color="#0000ff"),
        #             tickfont=dict(color='#0000ff'),
        #             overlaying='y', side='left', position=0.06, showline=True, linecolor='#0000ff')
    )
    # fig.update_layout( )

    fig.show()

    print("S")


if __name__ == '__main__':
    features = get_features('000001', start_date='20200101', end_date='20240501')
    plot_skdj_avg(features)

    #
    # daily = load_table('daily', ts_code=ts_code, start_date=start_date, end_date=end_date)
    # daily['trade_date'] = daily['trade_date'].map(lambda date: str(date))
