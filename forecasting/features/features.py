"""
特征值计算

"""
import pandas as pd
from forecasting.utils.formula import SKDJ

from forecasting.utils.utils import load_table
import chart_studio.plotly as py
import plotly.express as px
import plotly.graph_objects as go


def get_features(daily):
    daily['AVG'] = (daily['amount'] * 10) / daily['vol']
    skdj = SKDJ(daily, 36, 5)




    return pd.concat([daily[['trade_date', 'AVG']], skdj], axis=1)


def plot_skdj_avg(df):
    trace1 = go.Scatter(x=df['trade_date'], y=df['SKDJ_K'], name='SKDJ', xaxis='x', yaxis='y1')
    trace2 = go.Scatter(x=df['trade_date'], y=df['SKDJ_D'], name='SKDJ', xaxis='x', yaxis='y1')
    trace3 = go.Scatter(x=df['trade_date'], y=df['AVG'], name='AVG', xaxis='x', yaxis='y2')

    data = [trace1, trace2, trace3]
    layout = go.Layout(yaxis2=dict(anchor='x', overlaying='y', side='right'))

    fig = go.Figure(data=data, layout=layout)
    fig.show()

    print("S")


if __name__ == '__main__':
    ts_code = '000001.SZ'
    start_date = '20210101'
    end_date = '20240501'
    daily = load_table('daily', ts_code=ts_code, start_date=start_date, end_date=end_date)
    daily['trade_date'] = daily['trade_date'].map(lambda date: str(date))

    df = get_features(daily)
    plot_skdj_avg(df)
