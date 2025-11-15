
import numpy as np
import pandas as pd
import vectorbt as vbt

"""
Vectorbt 简单信号函数版（50种策略模板）
每个函数输入 DataFrame(df) 含至少 'close', 'high', 'low', 'volume'
每个函数输出 pd.Series 信号：+1=买入, 0=空仓, -1=卖出
"""

# === 一、趋势追踪类 ===
def signal_ma_cross(df, short=10, long=30):
    short_ma = df['close'].rolling(short).mean()
    long_ma = df['close'].rolling(long).mean()
    return pd.Series(np.where(short_ma > long_ma, 1, -1), index=df.index)

def signal_triple_ma(df, ma1=5, ma2=20, ma3=60):
    cond = (df['close'].rolling(ma1).mean() > df['close'].rolling(ma2).mean()) & \
           (df['close'].rolling(ma2).mean() > df['close'].rolling(ma3).mean())
    return pd.Series(np.where(cond, 1, -1), index=df.index)

def signal_momentum(df, period=10):
    mom = df['close'].pct_change(period)
    return pd.Series(np.sign(mom), index=df.index)

def signal_channel_break(df, n=20):
    up = df['high'].rolling(n).max()
    low = df['low'].rolling(n).min()
    return pd.Series(np.where(df['close'] > up, 1, np.where(df['close'] < low, -1, 0)), index=df.index)

def signal_macd(df, fast=12, slow=26, signal=9):
    ema_fast = df['close'].ewm(span=fast).mean()
    ema_slow = df['close'].ewm(span=slow).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal).mean()
    return pd.Series(np.where(dif > dea, 1, -1), index=df.index)

def signal_trend_strength(df, n=14):
    ret = df['close'].pct_change().rolling(n).sum()
    return pd.Series(np.where(ret > 0, 1, -1), index=df.index)


# === 二、均值回归类 ===
def signal_boll(df, n=20, k=2):
    ma = df['close'].rolling(n).mean()
    std = df['close'].rolling(n).std()
    upper = ma + k * std
    lower = ma - k * std
    return pd.Series(np.where(df['close'] < lower, 1, np.where(df['close'] > upper, -1, 0)), index=df.index)

def signal_rsi(df, n=14):
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.rolling(n).mean() / loss.rolling(n).mean()
    rsi = 100 - 100 / (1 + rs)
    return pd.Series(np.where(rsi < 30, 1, np.where(rsi > 70, -1, 0)), index=df.index)

def signal_kdj(df, n=9):
    low = df['low'].rolling(n).min()
    high = df['high'].rolling(n).max()
    rsv = (df['close'] - low) / (high - low) * 100
    k = rsv.ewm(com=2).mean()
    d = k.ewm(com=2).mean()
    return pd.Series(np.where(k > d, 1, -1), index=df.index)

def signal_vwap(df):
    vwap = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
    return pd.Series(np.where(df['close'] < vwap, 1, -1), index=df.index)

def signal_mean_reversion(df, n=20):
    ma = df['close'].rolling(n).mean()
    return pd.Series(np.where(df['close'] < ma, 1, -1), index=df.index)


# === 三、动量与混合类 ===
def signal_dual_momentum(df, lookback=60):
    ret = df['close'].pct_change(lookback)
    return pd.Series(np.sign(ret), index=df.index)

def signal_rsi_ma(df):
    s1 = signal_rsi(df)
    s2 = signal_ma_cross(df)
    return pd.Series(np.where((s1 == 1) & (s2 == 1), 1, np.where((s1 == -1) & (s2 == -1), -1, 0)), index=df.index)

def signal_momentum_rank(df, window=20):
    rank = df['close'].pct_change(window).rank(pct=True)
    return pd.Series(np.where(rank > 0.7, 1, np.where(rank < 0.3, -1, 0)), index=df.index)


# === 四、因子选股类 ===
def signal_value_factor(pe):
    return pd.Series(np.where(pe < pe.median(), 1, -1), index=pe.index)

def signal_profit_factor(roe):
    return pd.Series(np.where(roe > roe.median(), 1, -1), index=roe.index)

def signal_growth_factor(eps_growth):
    return pd.Series(np.where(eps_growth > eps_growth.median(), 1, -1), index=eps_growth.index)

def signal_size_factor(market_cap):
    return pd.Series(np.where(market_cap < market_cap.median(), 1, -1), index=market_cap.index)

def signal_volatility_factor(vol):
    return pd.Series(np.where(vol < vol.median(), 1, -1), index=vol.index)

def signal_leverage_factor(de_ratio):
    return pd.Series(np.where(de_ratio < de_ratio.median(), 1, -1), index=de_ratio.index)

def signal_liquidity_factor(turnover):
    return pd.Series(np.where(turnover > turnover.median(), 1, -1), index=turnover.index)

def signal_surprise_factor(eps_actual, eps_exp):
    diff = eps_actual - eps_exp
    return pd.Series(np.where(diff > 0, 1, -1), index=diff.index)

def signal_beta_factor(beta):
    return pd.Series(np.where(beta < beta.median(), 1, -1), index=beta.index)


# === 五、事件驱动类 ===
def signal_earnings_event(eps_actual, eps_est):
    return pd.Series(np.where(eps_actual > eps_est, 1, -1), index=eps_est.index)

def signal_dividend_play(div_yield):
    return pd.Series(np.where(div_yield > div_yield.median(), 1, -1), index=div_yield.index)

def signal_buyback_flag(flag_series):
    return pd.Series(np.where(flag_series == 1, 1, 0), index=flag_series.index)

def signal_policy_news(score):
    return pd.Series(np.where(score > 0, 1, -1), index=score.index)


# === 六、资金与情绪类 ===
def signal_north_flow(flow_series):
    return pd.Series(np.where(flow_series > 0, 1, -1), index=flow_series.index)

def signal_big_order(flow_series):
    return pd.Series(np.where(flow_series > flow_series.rolling(5).mean(), 1, -1), index=flow_series.index)

def signal_news_sentiment(sentiment_score):
    return pd.Series(np.where(sentiment_score > 0, 1, -1), index=sentiment_score.index)

def signal_social_sentiment(score):
    return pd.Series(np.where(score > score.median(), 1, -1), index=score.index)

def signal_volume_spike(df, mult=2):
    vol_mean = df['volume'].rolling(20).mean()
    return pd.Series(np.where(df['volume'] > vol_mean * mult, 1, 0), index=df.index)


if __name__ == "__main__":
    df = pd.DataFrame()
    entries = signal_ma_cross(df) == 1
    exits = signal_ma_cross(df) == -1
    pf = vbt.Portfolio.from_signals(df['close'], entries, exits)
    pf.stats()