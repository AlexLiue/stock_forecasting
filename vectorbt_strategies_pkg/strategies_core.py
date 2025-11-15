import vectorbt as vbt
import pandas as pd
import numpy as np


#############################################
#             趋势追踪类（8个）
#############################################

# 1. 均线交叉策略
MA_Cross = vbt.IndicatorFactory(
    class_name='MA_Cross',
    short_name='ma_cross',
    input_names=['close'],
    param_names=['short_window', 'long_window'],
    output_names=['entries', 'exits']
).from_apply_func(
    lambda close, short_window=10, long_window=50: (
        print("运行策略：MA_Cross"),
        vbt.MA.run(close, short_window).ma_crossed_above(vbt.MA.run(close, long_window).ma),
        vbt.MA.run(close, short_window).ma_crossed_below(vbt.MA.run(close, long_window).ma)
    )[1:],
)

# 2. MACD 趋势策略
MACD_Trend = vbt.IndicatorFactory.from_preset("macd")

# 3. 布林带趋势突破
def bb_trend_apply(close, window=20, std=2):
    print("运行策略：Bollinger_Breakout")
    bb = vbt.BBANDS.run(close, window=window, std=std)
    entries = close > bb.upper
    exits = close < bb.lower
    return entries, exits

BB_Trend = vbt.IndicatorFactory(
    class_name='BB_Trend',
    short_name='bb_trend',
    input_names=['close'],
    param_names=['window', 'std'],
    output_names=['entries', 'exits']
).from_apply_func(bb_trend_apply)

# 4. ATR 趋势跟踪突破
def atr_breakout_apply(close, window=14, multiplier=3.0):
    print("运行策略：ATR_Breakout")
    atr = vbt.ATR.run(high=close, low=close, close=close, window=window)
    upper = close + atr.atr * multiplier
    lower = close - atr.atr * multiplier
    entries = close > upper
    exits = close < lower
    return entries, exits

ATR_Breakout = vbt.IndicatorFactory(
    class_name='ATR_Breakout',
    short_name='atr_break',
    input_names=['close'],
    param_names=['window', 'multiplier'],
    output_names=['entries', 'exits']
).from_apply_func(atr_breakout_apply)

# 5. Donchian 通道突破
def donchian_apply(close, window=20):
    print("运行策略：Donchian_Channel")
    upper = close.rolling(window).max()
    lower = close.rolling(window).min()
    entries = close > upper.shift(1)
    exits = close < lower.shift(1)
    return entries, exits

Donchian_Channel = vbt.IndicatorFactory(
    class_name='Donchian_Channel',
    short_name='donch',
    input_names=['close'],
    param_names=['window'],
    output_names=['entries', 'exits']
).from_apply_func(donchian_apply)

# 6. Keltner 通道突破
def keltner_apply(close, window=20, multiplier=1.5):
    print("运行策略：Keltner_Channel")
    ema = close.ewm(span=window).mean()
    atr = vbt.ATR.run(high=close, low=close, close=close, window=window).atr
    upper = ema + multiplier * atr
    lower = ema - multiplier * atr
    entries = close > upper
    exits = close < lower
    return entries, exits

Keltner_Channel = vbt.IndicatorFactory(
    class_name='Keltner_Channel',
    short_name='kc',
    input_names=['close'],
    param_names=['window', 'multiplier'],
    output_names=['entries', 'exits']
).from_apply_func(keltner_apply)

# 7. SuperTrend
def supertrend_apply(close, window=10, multiplier=3):
    print("运行策略：SuperTrend")
    atr = vbt.ATR.run(high=close, low=close, close=close, window=window).atr
    upper = close + atr * multiplier
    lower = close - atr * multiplier
    trend = np.where(close > upper, 1, np.where(close < lower, -1, np.nan))
    trend = pd.Series(trend).ffill()
    entries = trend == 1
    exits = trend == -1
    return entries, exits

SuperTrend = vbt.IndicatorFactory(
    class_name='SuperTrend',
    short_name='super',
    input_names=['close'],
    param_names=['window', 'multiplier'],
    output_names=['entries', 'exits']
).from_apply_func(supertrend_apply)

# 8. ADX 趋势强度策略
def adx_follow_apply(high, low, close, window=14):
    print("运行策略：ADX_Follow")
    adx = vbt.ADX.run(high=high, low=low, close=close, window=window)
    entries = adx.plus_di > adx.minus_di
    exits = adx.plus_di < adx.minus_di
    return entries, exits

ADX_Follow = vbt.IndicatorFactory(
    class_name='ADX_Follow',
    short_name='adx_follow',
    input_names=['high', 'low', 'close'],
    param_names=['window'],
    output_names=['entries', 'exits']
).from_apply_func(adx_follow_apply)

#############################################
#             均值回归类（8个）
#############################################

def rsi_revert_apply(close, window=14, low_th=30, high_th=70):
    print("运行策略：RSI_MeanReversion")
    rsi = vbt.RSI.run(close, window=window).rsi
    entries = rsi < low_th
    exits = rsi > high_th
    return entries, exits

RSI_MeanReversion = vbt.IndicatorFactory(
    class_name='RSI_MeanReversion',
    short_name='rsi_rev',
    input_names=['close'],
    param_names=['window', 'low_th', 'high_th'],
    output_names=['entries', 'exits']
).from_apply_func(rsi_revert_apply)

# （以下略: 其余均值回归、动量、因子、事件、情绪共42个策略将在 Part 2 输出）

#############################################
#           均值回归类（续）
#############################################

# 2. 布林带均值回归
def bb_revert_apply(close, window=20, std=2):
    print("运行策略：BB_MeanReversion")
    bb = vbt.BBANDS.run(close, window=window, std=std)
    entries = close < bb.lower
    exits = close > bb.upper
    return entries, exits

BB_MeanReversion = vbt.IndicatorFactory(
    class_name='BB_MeanReversion',
    short_name='bb_rev',
    input_names=['close'],
    param_names=['window','std'],
    output_names=['entries','exits']
).from_apply_func(bb_revert_apply)

# 3. 均线差均值回归
def ma_spread_revert_apply(close, short_window=10, long_window=50):
    print("运行策略：MA_SpreadReversion")
    short = vbt.MA.run(close, short_window).ma
    long = vbt.MA.run(close, long_window).ma
    spread = short - long
    entries = spread < spread.mean()
    exits = spread > spread.mean()
    return entries, exits

MA_SpreadReversion = vbt.IndicatorFactory(
    class_name='MA_SpreadReversion',
    short_name='ma_spread_rev',
    input_names=['close'],
    param_names=['short_window','long_window'],
    output_names=['entries','exits']
).from_apply_func(ma_spread_revert_apply)

# 4. Keltner 通道均值回归
def keltner_rev_apply(close, window=20, multiplier=1.5):
    print("运行策略：Keltner_MeanReversion")
    ema = close.ewm(span=window).mean()
    atr = vbt.ATR.run(high=close, low=close, close=close, window=window).atr
    upper = ema + multiplier * atr
    lower = ema - multiplier * atr
    entries = close < lower
    exits = close > upper
    return entries, exits

Keltner_Revert = vbt.IndicatorFactory(
    class_name='Keltner_Revert',
    short_name='kc_rev',
    input_names=['close'],
    param_names=['window','multiplier'],
    output_names=['entries','exits']
).from_apply_func(keltner_rev_apply)

# 5. ZScore 均值回归
def zscore_revert_apply(close, window=20, threshold=2):
    print("运行策略：ZScore_Reversion")
    mean = close.rolling(window).mean()
    std = close.rolling(window).std()
    z = (close - mean) / std
    entries = z < -threshold
    exits = z > threshold
    return entries, exits

ZScore_Reversion = vbt.IndicatorFactory(
    class_name='ZScore_Reversion',
    short_name='z_rev',
    input_names=['close'],
    param_names=['window','threshold'],
    output_names=['entries','exits']
).from_apply_func(zscore_revert_apply)

# 6. RSI 均值回归反转（高速版）
def rsi_flip_apply(close, fast=6, low_th=30, high_th=70):
    print("运行策略：RSI_Flip")
    rsi = vbt.RSI.run(close, window=fast).rsi
    entries = rsi < low_th
    exits = rsi > high_th
    return entries, exits

RSI_Flip = vbt.IndicatorFactory(
    class_name='RSI_Flip',
    short_name='rsi_flip',
    input_names=['close'],
    param_names=['fast','low_th','high_th'],
    output_names=['entries','exits']
).from_apply_func(rsi_flip_apply)

# 7. 均值回复带价差阈值
def spread_threshold_apply(close, window=15, threshold=0.03):
    print("运行策略：Spread_Threshold")
    mean = close.rolling(window).mean()
    entries = (close / mean - 1) < -threshold
    exits = (close / mean - 1) > threshold
    return entries, exits

Spread_Threshold = vbt.IndicatorFactory(
    class_name='Spread_Threshold',
    short_name='spread_th',
    input_names=['close'],
    param_names=['window','threshold'],
    output_names=['entries','exits']
).from_apply_func(spread_threshold_apply)

# 8. 乖离率均值回归
def bias_revert_apply(close, window=20, threshold=0.05):
    print("运行策略：Bias_Reversion")
    ma = close.rolling(window).mean()
    bias = (close - ma) / ma
    entries = bias < -threshold
    exits = bias > threshold
    return entries, exits

Bias_Reversion = vbt.IndicatorFactory(
    class_name='Bias_Reversion',
    short_name='bias_rev',
    input_names=['close'],
    param_names=['window','threshold'],
    output_names=['entries','exits']
).from_apply_func(bias_revert_apply)

#############################################
#             动量类（8个）
#############################################

def roc_momentum_apply(close, window=12, threshold=0):
    print("运行策略：ROC_Momentum")
    roc = close.pct_change(window)
    entries = roc > threshold
    exits = roc < -threshold
    return entries, exits
ROC_Momentum = vbt.IndicatorFactory(
    class_name='ROC_Momentum', short_name='roc',
    input_names=['close'], param_names=['window','threshold'],
    output_names=['entries','exits']
).from_apply_func(roc_momentum_apply)

def sma_momentum_apply(close, window=20):
    print("运行策略：SMA_Momentum")
    sma = close.rolling(window).mean()
    entries = close > sma
    exits = close < sma
    return entries, exits
SMA_Momentum = vbt.IndicatorFactory(
    class_name='SMA_Momentum', short_name='sma_mom',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(sma_momentum_apply)

def breakout_mom_apply(close, window=55):
    print("运行策略：Breakout_Momentum")
    maxp = close.rolling(window).max()
    entries = close > maxp
    exits = close < maxp.shift(1)
    return entries, exits
Breakout_Momentum = vbt.IndicatorFactory(
    class_name='Breakout_Momentum', short_name='break_mom',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(breakout_mom_apply)

def momentum_rank_apply(close, window=20):
    print("运行策略：Momentum_Rank")
    ret = close.pct_change(window)
    entries = ret > 0
    exits = ret < 0
    return entries, exits
Momentum_Rank = vbt.IndicatorFactory(
    class_name='Momentum_Rank', short_name='mom_rank',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(momentum_rank_apply)

def rsi_momentum_apply(close, window=14, high_th=60):
    print("运行策略：RSI_Momentum")
    rsi = vbt.RSI.run(close, window=window).rsi
    entries = rsi > high_th
    exits = rsi < 50
    return entries, exits
RSI_Momentum = vbt.IndicatorFactory(
    class_name='RSI_Momentum', short_name='rsi_mom',
    input_names=['close'], param_names=['window','high_th'],
    output_names=['entries','exits']
).from_apply_func(rsi_momentum_apply)

def macd_momentum_apply(close, fast=12, slow=26, signal=9):
    print("运行策略：MACD_Momentum")
    macd = vbt.MACD.run(close, fast, slow, signal)
    entries = macd.macd > macd.signal
    exits = macd.macd < macd.signal
    return entries, exits
MACD_Momentum = vbt.IndicatorFactory(
    class_name='MACD_Momentum', short_name='macd_mom',
    input_names=['close'], param_names=['fast','slow','signal'],
    output_names=['entries','exits']
).from_apply_func(macd_momentum_apply)

def stochastic_mom_apply(close, k_window=14, d_window=3, low_th=20, high_th=80):
    print("运行策略：Stochastic_Momentum")
    stoch = vbt.STOCH.run(close, close, close, k_window, d_window)
    entries = stoch.percent_k > high_th
    exits = stoch.percent_k < low_th
    return entries, exits
Stochastic_Momentum = vbt.IndicatorFactory(
    class_name='Stochastic_Momentum', short_name='stoch_mom',
    input_names=['close'], param_names=['k_window','d_window','low_th','high_th'],
    output_names=['entries','exits']
).from_apply_func(stochastic_mom_apply)

def rate_acceleration_apply(close, window=20):
    print("运行策略：Rate_Acceleration")
    roc = close.pct_change(window)
    accel = roc.diff()
    entries = accel > 0
    exits = accel < 0
    return entries, exits
Rate_Acceleration = vbt.IndicatorFactory(
    class_name='Rate_Acceleration', short_name='rate_ac',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(rate_acceleration_apply)

#############################################
#           因子类（8个）
#############################################

def volatility_factor_apply(close, window=20, threshold=0.02):
    print("运行策略：Volatility_Factor")
    vol = close.pct_change().rolling(window).std()
    entries = vol < threshold
    exits = vol > threshold
    return entries, exits
Volatility_Factor = vbt.IndicatorFactory(
    class_name='Volatility_Factor', short_name='vol_factor',
    input_names=['close'], param_names=['window','threshold'],
    output_names=['entries','exits']
).from_apply_func(volatility_factor_apply)

def momentum_factor_apply(close, window=20):
    print("运行策略：Momentum_Factor")
    ret = close.pct_change(window)
    entries = ret > 0
    exits = ret < 0
    return entries, exits
Momentum_Factor = vbt.IndicatorFactory(
    class_name='Momentum_Factor', short_name='mom_factor',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(momentum_factor_apply)

def mean_reversal_factor_apply(close, window=10):
    print("运行策略：MeanReversal_Factor")
    mean = close.rolling(window).mean()
    entries = close < mean
    exits = close > mean
    return entries, exits
MeanReversal_Factor = vbt.IndicatorFactory(
    class_name='MeanReversal_Factor', short_name='mr_factor',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(mean_reversal_factor_apply)

def turnover_factor_apply(close, volume, window=10):
    print("运行策略：Turnover_Factor")
    entries = volume.rolling(window).mean() > volume.shift(1)
    exits = volume.rolling(window).mean() < volume.shift(1)
    return entries, exits
Turnover_Factor = vbt.IndicatorFactory(
    class_name='Turnover_Factor', short_name='turn_factor',
    input_names=['close','volume'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(turnover_factor_apply)

def volatility_surge_apply(close, window=10, ratio=1.5):
    print("运行策略：Volatility_Surge")
    vol = close.pct_change().rolling(window).std()
    mean = vol.rolling(window).mean()
    entries = vol > mean * ratio
    exits = vol < mean
    return entries, exits
Volatility_Surge = vbt.IndicatorFactory(
    class_name='Volatility_Surge', short_name='vol_surge',
    input_names=['close'], param_names=['window','ratio'],
    output_names=['entries','exits']
).from_apply_func(volatility_surge_apply)

def volume_spike_apply(close, volume, window=5, ratio=2):
    print("运行策略：Volume_Spike")
    mean_vol = volume.rolling(window).mean()
    entries = volume > mean_vol * ratio
    exits = volume < mean_vol
    return entries, exits
Volume_Spike = vbt.IndicatorFactory(
    class_name='Volume_Spike', short_name='vol_spike',
    input_names=['close','volume'], param_names=['window','ratio'],
    output_names=['entries','exits']
).from_apply_func(volume_spike_apply)

def price_volume_factor_apply(close, volume, window=10):
    print("运行策略：PriceVolume_Factor")
    pv = close.pct_change(window) * volume.rolling(window).mean()
    entries = pv > pv.shift(1)
    exits = pv < pv.shift(1)
    return entries, exits
PriceVolume_Factor = vbt.IndicatorFactory(
    class_name='PriceVolume_Factor', short_name='pv_factor',
    input_names=['close','volume'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(price_volume_factor_apply)

def sharpe_factor_apply(close, window=20):
    print("运行策略：Sharpe_Factor")
    ret = close.pct_change()
    sharpe = ret.rolling(window).mean() / ret.rolling(window).std()
    entries = sharpe > 0
    exits = sharpe < 0
    return entries, exits
Sharpe_Factor = vbt.IndicatorFactory(
    class_name='Sharpe_Factor', short_name='sharpe_f',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(sharpe_factor_apply)

#############################################
#           事件驱动类（8个）
#############################################

def gap_open_apply(open_, close_prev, thresh=0.02):
    print("运行策略：Gap_Open")
    gap = (open_ - close_prev) / close_prev
    entries = gap > thresh
    exits = gap < -thresh
    return entries, exits
Gap_Open = vbt.IndicatorFactory(
    class_name='Gap_Open', short_name='gap',
    input_names=['open_','close_prev'], param_names=['thresh'],
    output_names=['entries','exits']
).from_apply_func(gap_open_apply)

def earnings_event_apply(close, events):
    print("运行策略：Earnings_Event")
    entries = events == 1
    exits = events == -1
    return entries, exits
Earnings_Event = vbt.IndicatorFactory(
    class_name='Earnings_Event', short_name='earn_evt',
    input_names=['close','events'], param_names=[],
    output_names=['entries','exits']
).from_apply_func(earnings_event_apply)

def volume_shock_apply(volume, window=20, ratio=3):
    print("运行策略：Volume_Shock")
    mean_vol = volume.rolling(window).mean()
    entries = volume > mean_vol * ratio
    exits = volume < mean_vol
    return entries, exits
Volume_Shock = vbt.IndicatorFactory(
    class_name='Volume_Shock', short_name='vol_shock',
    input_names=['volume'], param_names=['window','ratio'],
    output_names=['entries','exits']
).from_apply_func(volume_shock_apply)

def volatility_event_apply(close, window=10, ratio=2):
    print("运行策略：Volatility_Event")
    vol = close.pct_change().rolling(window).std()
    mean = vol.rolling(window).mean()
    entries = vol > mean * ratio
    exits = vol < mean
    return entries, exits
Volatility_Event = vbt.IndicatorFactory(
    class_name='Volatility_Event', short_name='vol_evt',
    input_names=['close'], param_names=['window','ratio'],
    output_names=['entries','exits']
).from_apply_func(volatility_event_apply)

def moving_avg_crossover_event_apply(close, short=10, long=50):
    print("运行策略：MA_Cross_Event")
    ma_short = close.rolling(short).mean()
    ma_long = close.rolling(long).mean()
    entries = ma_short > ma_long
    exits = ma_short < ma_long
    return entries, exits
MA_Cross_Event = vbt.IndicatorFactory(
    class_name='MA_Cross_Event', short_name='ma_evt',
    input_names=['close'], param_names=['short','long'],
    output_names=['entries','exits']
).from_apply_func(moving_avg_crossover_event_apply)

def breakout_event_apply(close, window=20):
    print("运行策略：Breakout_Event")
    high = close.rolling(window).max()
    entries = close > high
    exits = close < high.shift(1)
    return entries, exits
Breakout_Event = vbt.IndicatorFactory(
    class_name='Breakout_Event', short_name='br_evt',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(breakout_event_apply)

def price_shock_apply(close, window=5, ratio=0.05):
    print("运行策略：Price_Shock")
    ret = close.pct_change(window)
    entries = ret > ratio
    exits = ret < -ratio
    return entries, exits
Price_Shock = vbt.IndicatorFactory(
    class_name='Price_Shock', short_name='price_shk',
    input_names=['close'], param_names=['window','ratio'],
    output_names=['entries','exits']
).from_apply_func(price_shock_apply)

def reversal_event_apply(close, window=3):
    print("运行策略：Reversal_Event")
    ret = close.pct_change(window)
    entries = ret < 0
    exits = ret > 0
    return entries, exits
Reversal_Event = vbt.IndicatorFactory(
    class_name='Reversal_Event', short_name='rev_evt',
    input_names=['close'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(reversal_event_apply)

#############################################
#           情绪类（8个）
#############################################

def sentiment_ma_apply(sentiment, window=10):
    print("运行策略：Sentiment_MA")
    ma = sentiment.rolling(window).mean()
    entries = sentiment > ma
    exits = sentiment < ma
    return entries, exits
Sentiment_MA = vbt.IndicatorFactory(
    class_name='Sentiment_MA', short_name='sent_ma',
    input_names=['sentiment'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(sentiment_ma_apply)

def fear_greed_apply(index, th_low=30, th_high=70):
    print("运行策略：FearGreed_Index")
    entries = index > th_high
    exits = index < th_low
    return entries, exits
FearGreed_Index = vbt.IndicatorFactory(
    class_name='FearGreed_Index', short_name='fg_idx',
    input_names=['index'], param_names=['th_low','th_high'],
    output_names=['entries','exits']
).from_apply_func(fear_greed_apply)

def news_sentiment_apply(sentiment, threshold=0):
    print("运行策略：News_Sentiment")
    entries = sentiment > threshold
    exits = sentiment < threshold
    return entries, exits
News_Sentiment = vbt.IndicatorFactory(
    class_name='News_Sentiment', short_name='news_sent',
    input_names=['sentiment'], param_names=['threshold'],
    output_names=['entries','exits']
).from_apply_func(news_sentiment_apply)

def twitter_sentiment_apply(sentiment, pos_th=0.2, neg_th=-0.2):
    print("运行策略：Twitter_Sentiment")
    entries = sentiment > pos_th
    exits = sentiment < neg_th
    return entries, exits
Twitter_Sentiment = vbt.IndicatorFactory(
    class_name='Twitter_Sentiment', short_name='twt_sent',
    input_names=['sentiment'], param_names=['pos_th','neg_th'],
    output_names=['entries','exits']
).from_apply_func(twitter_sentiment_apply)

def reddit_sentiment_apply(sentiment, pos_th=0.3, neg_th=-0.3):
    print("运行策略：Reddit_Sentiment")
    entries = sentiment > pos_th
    exits = sentiment < neg_th
    return entries, exits
Reddit_Sentiment = vbt.IndicatorFactory(
    class_name='Reddit_Sentiment', short_name='redd_sent',
    input_names=['sentiment'], param_names=['pos_th','neg_th'],
    output_names=['entries','exits']
).from_apply_func(reddit_sentiment_apply)

def social_sentiment_apply(sentiment, window=5):
    print("运行策略：Social_Sentiment")
    avg = sentiment.rolling(window).mean()
    entries = sentiment > avg
    exits = sentiment < avg
    return entries, exits
Social_Sentiment = vbt.IndicatorFactory(
    class_name='Social_Sentiment', short_name='soc_sent',
    input_names=['sentiment'], param_names=['window'],
    output_names=['entries','exits']
).from_apply_func(social_sentiment_apply)

def combined_sentiment_apply(sentiment, threshold=0):
    print("运行策略：Combined_Sentiment")
    entries = sentiment > threshold
    exits = sentiment < 0
    return entries, exits
Combined_Sentiment = vbt.IndicatorFactory(
    class_name='Combined_Sentiment', short_name='comb_sent',
    input_names=['sentiment'], param_names=['threshold'],
    output_names=['entries','exits']
).from_apply_func(combined_sentiment_apply)

def sentiment_vol_apply(sentiment, vol, th=0.02):
    print("运行策略：Sentiment_Volatility")
    entries = (sentiment > 0) & (vol < th)
    exits = (sentiment < 0) | (vol > th)
    return entries, exits
Sentiment_Volatility = vbt.IndicatorFactory(
    class_name='Sentiment_Volatility', short_name='sent_vol',
    input_names=['sentiment','vol'], param_names=['th'],
    output_names=['entries','exits']
).from_apply_func(sentiment_vol_apply)
