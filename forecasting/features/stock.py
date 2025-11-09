import logging
import time
from collections import OrderedDict

import plotly.offline

from forecasting.utils.utils import (
    get_mock_connection,
    load_table,
    get_cfg,
    get_logger,
    timedelta_to_str,
    get_query_condition,
)
from forecasting.utils.formula import *
from plotly import graph_objs as go
from plotly.offline import init_notebook_mode

init_notebook_mode(connected=True)

pd.set_option("display.max_columns", None)


class LazyProperty:
    """
    使用 @LazyProperty 装饰器，将类中的一个方法声明为懒加载属性。
    当访问该属性时，会调用该方法计算并返回结果，而后续访问将返回相同的结果。
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value


class Stock:
    def __init__(self, period, symbol, start_date, end_date):  # 股票代码
        self.period = period
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.name = self.name

    @LazyProperty
    def cfg(self):
        return get_cfg()

    @LazyProperty
    def logger(self):
        return get_logger(f"forecast-{self.symbol}")

    @LazyProperty
    def engine(self):
        return get_mock_connection()

    @LazyProperty
    def name(self):
        query_sql = (
            f"SELECT sbi.`name` "
            f"FROM {self.cfg['mysql']['database']}.stock_basic_info sbi "
            f"WHERE symbol={self.symbol}"
        )
        self.logger.info(f"Execute SQL [{query_sql}]")
        return pd.read_sql(query_sql, self.engine).iloc[0, 0]

    @LazyProperty
    def df(self):
        if self.period == "day":
            condition = get_query_condition(
                symbol=self.symbol, start_date=self.start_date, end_date=self.end_date
            )
            sql = (
                f"SELECT * FROM {self.cfg['mysql']['database']}.stock_daily_qfq t "
                f"WHERE {condition} ORDER BY symbol, trade_date"
            )
            self.logger.info(f"Execute SQL  [{sql}]")
            return pd.read_sql(sql, self.engine)
        elif self.period == "min30":
            condition = get_query_condition(
                symbol=self.symbol, start_date=self.start_date, end_date=self.end_date
            )
            sql = (
                f"SELECT * FROM {self.cfg['mysql']['database']}.stock_min30_qfq t "
                f"WHERE {condition} ORDER BY symbol, trade_date, trade_time"
            )
            self.logger.info(f"Execute SQL  [{sql}]")
            return pd.read_sql(sql, self.engine)
        else:
            self.logger.ERROR(
                f"Period[{self.period}] Error, Only support Period[day, min30]..."
            )

    @LazyProperty
    def TRADE_DATE(self):
        TRADE_DATE = None
        if "trade_time" in self.df.columns:
            TRADE_DATE = pd.to_datetime(
                self.df["trade_date"].map(lambda d: str(d.strftime("%Y-%m-%d ")))
                + self.df["trade_time"].map(lambda t: timedelta_to_str(t))
            )
        else:
            TRADE_DATE = pd.to_datetime(
                self.df["trade_date"].map(lambda d: str(d.strftime("%Y-%m-%d ")))
                + "12:00:00"
            )
        DICT = {"TRADE_DATE": TRADE_DATE}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def AVG(self):
        AVG = self.df["amount"] / self.df["vol"] / 100
        DICT = {"AVG": AVG}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def C_MACD_12_26_9(self):
        RS = MACD(DF=self.AVG["AVG"], FAST=12, SLOW=26, MID=9)[["DIFF", "DEA", "MACD"]]
        RS.columns = [["DIFF_12_26_9", "DEA_12_26_9", "MACD_12_26_9"]]
        return RS

    @LazyProperty
    def C_SKDJ_36_5(self):
        RS = SKDJ(DF=self.df, N=36, M=5)[["SKDJ_K", "SKDJ_D"]]
        RS.columns = [["SKDJ_K_36_5", "SKDJ_D_36_5"]]
        return RS

    @LazyProperty
    def DIFF_12_26_9(self):
        DIFF_12_26_9 = np.squeeze(self.C_MACD_12_26_9["DIFF_12_26_9"])
        DICT = {"DIFF_12_26_9": DIFF_12_26_9}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def DEA_12_26_9(self):
        DEA_12_26_9 = np.squeeze(self.C_MACD_12_26_9["DEA_12_26_9"])
        DICT = {"DEA_12_26_9": DEA_12_26_9}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def MACD_12_26_9(self):
        MACD_12_26_9 = np.squeeze(self.C_MACD_12_26_9["MACD_12_26_9"])
        DICT = {"MACD_12_26_9": MACD_12_26_9}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def SKDJ_K_36_5(self):
        SKDJ_K_36_5 = np.squeeze(self.C_SKDJ_36_5["SKDJ_K_36_5"])
        DICT = {"SKDJ_K_36_5": SKDJ_K_36_5}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def SKDJ_D_36_5(self):
        SKDJ_D_36_5 = np.squeeze(self.C_SKDJ_36_5["SKDJ_D_36_5"])
        DICT = {"SKDJ_D_36_5": SKDJ_D_36_5}
        RS = pd.DataFrame(DICT)
        return RS

    @LazyProperty
    def FT(self):
        VAR = pd.concat(
            [
                self.TRADE_DATE,
                self.AVG,
                self.DIFF_12_26_9,
                self.DEA_12_26_9,
                self.MACD_12_26_9,
                self.SKDJ_K_36_5,
                self.SKDJ_D_36_5,
            ],
            axis=1,
        )
        return VAR

    def plot(self):
        FT = self.FT.dropna().reset_index(drop=True)
        fig = go.Figure()
        dt = FT["TRADE_DATE"]
        # dt = list(range(FT['TRADE_DATE'].shape[0]))
        fig.add_trace(go.Scatter(x=dt, y=FT["AVG"], name="AVG", xaxis="x"))
        fig.add_trace(
            go.Scatter(
                x=dt, y=FT["DIFF_12_26_9"], name="DIFF_12_26_9", xaxis="x", yaxis="y2"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dt, y=FT["DEA_12_26_9"], name="DEA_12_26_9", xaxis="x", yaxis="y2"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dt, y=FT["MACD_12_26_9"], name="MACD_12_26_9", xaxis="x", yaxis="y2"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=dt, y=FT["SKDJ_K_36_5"], name="SKDJ_K_36_5", xaxis="x", yaxis="y3"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dt, y=FT["SKDJ_D_36_5"], name="SKDJ_D_36_5", xaxis="x", yaxis="y3"
            )
        )

        fig.update_layout(
            margin=dict(l=10, r=10, b=40, t=40),
            title_text=f"{self.symbol} 特征参数",
            title_x=0.5,
            xaxis=dict(domain=[0.10, 0.98], showline=True),
            yaxis=dict(
                title=dict(text="AVG", standoff=0),
                titlefont=dict(color="#006400"),
                tickfont=dict(color="#006400"),
                side="right",
                position=1,
                showline=True,
                linecolor="#006400",
            ),
            yaxis2=dict(
                title=dict(text="MACD", standoff=0),
                titlefont=dict(color="#006400"),
                tickfont=dict(color="#006400"),
                overlaying="y",
                side="left",
                position=0.08,
                showline=True,
                linecolor="#006400",
            ),
            yaxis3=dict(
                title=dict(text="SKDJ", standoff=0),
                titlefont=dict(color="#006400"),
                tickfont=dict(color="#006400"),
                overlaying="y",
                side="left",
                position=0,
                showline=True,
                linecolor="#006400",
            ),
        )

        freq = "D" if self.period == "day" else "30min"
        dvalue = 24 * 60 * 60 * 1000 if self.period == "day" else 30 * 60 * 1000

        dt_date = [x.strftime("%Y-%m-%d %H:%M:%S") for x in dt]

        dt_all = pd.date_range(start=dt[0], end=dt[dt.shape[0] - 1], freq=freq)
        dt_all = [x for x in dt_all]

        dt_breaks = [
            d.strftime("%Y-%m-%d %H:%M:%S")
            for d in dt_all
            if d.strftime("%Y-%m-%d %H:%M:%S") not in dt_date
        ]

        fig.update_xaxes(
            rangebreaks=[dict(values=dt_breaks, dvalue=dvalue)],
            rangeselector=dict(
                buttons=list(
                    [
                        dict(
                            count=1, label="1m", step="month", stepmode="backward"
                        ),  # 往前推一个月
                        dict(
                            count=6, label="6m", step="month", stepmode="backward"
                        ),  # 往前推6个月
                        dict(
                            count=1, label="YTD", step="year", stepmode="todate"
                        ),  # 只显示今年数据
                        dict(
                            count=1, label="1y", step="year", stepmode="backward"
                        ),  # 显示过去一年的数据
                        dict(step="all"),  # 显示全部数据
                    ]
                )
            ),
        )

        fig.show()
        plotly.offline.plot(fig, filename="stock-plotly.html", auto_open=True)
