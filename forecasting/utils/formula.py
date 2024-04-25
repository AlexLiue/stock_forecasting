#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd


def EMA(DF, N):
    """
    指数平均数指标(Exponential Moving Average，EXPMA或EMA) 指数平均数指标也叫EXPMA指标, 对N日内的价格进行加权算术平均
    """
    return pd.Series.ewm(DF, span=N, min_periods=N - 1, adjust=True).mean()


def MA(DF, N):

    return pd.Series.rolling(DF, N).mean()


def SMA(DF, N, M):
    DF = DF.fillna(0)
    z = len(DF)
    var = np.zeros(z)
    var[0] = DF[0]
    for i in range(1, z):
        var[i] = (DF[i] * M + var[i - 1] * (N - M)) / N
    for i in range(z):
        DF[i] = var[i]
    return DF


def ATR(DF, N):
    C = DF['close']
    H = DF['high']
    L = DF['low']
    TR1 = MAX(MAX((H - L), ABS(REF(C, 1) - H)), ABS(REF(C, 1) - L))
    atr = MA(TR1, N)
    return atr


def HHV(DF, N):
    return pd.Series.rolling(DF, N).max()


def LLV(DF, N):
    return pd.Series.rolling(DF, N).min()


def SUM(DF, N):
    return pd.Series.rolling(DF, N).sum()


def ESUM(DF, N):
    return pd.Series.ewm(DF, span=N, min_periods=N - 1, adjust=True).sum()


def ABS(DF):
    return abs(DF)


def MAX(A, B):
    var = IF(A > B, A, B)
    return var


def MIN(A, B):
    var = IF(A < B, A, B)
    return var


def IF(COND, V1, V2):
    var = np.where(COND, V1, V2)
    for i in range(len(var)):
        V1[i] = var[i]
    return V1


def REF(DF, N):
    var = DF.diff(N)
    var = DF - var
    return var


def STD(DF, N):
    return pd.Series.rolling(DF, N).std()


def MACD(DF, FAST=12, SLOW=26, MID=9):
    """
    MACD平滑异同平均
    """
    EMAFAST = EMA(DF, FAST)
    EMASLOW = EMA(DF, SLOW)
    DIFF = EMAFAST - EMASLOW
    DEA = EMA(DIFF, MID)
    MACD = (DIFF - DEA) * 2
    DICT = {'DIFF': DIFF, 'DEA': DEA, 'MACD': MACD}
    VAR = pd.DataFrame(DICT)
    return VAR


def KDJ(DF, N, M1, M2):
    C = DF['close']
    H = DF['high']
    L = DF['low']
    RSV = (C - LLV(L, N)) / (HHV(H, N) - LLV(L, N)) * 100
    K = SMA(RSV, M1, 1)
    D = SMA(K, M2, 1)
    J = 3 * K - 2 * D
    DICT = {'KDJ_K': K, 'KDJ_D': D, 'KDJ_J': J}
    VAR = pd.DataFrame(DICT)
    return VAR


def OSC(DF, N, M):  # 变动速率线
    C = DF['close']
    OS = (C - MA(C, N)) * 100
    MAOSC = EMA(OS, M)
    DICT = {'OSC': OS, 'MAOSC': MAOSC}
    VAR = pd.DataFrame(DICT)
    return VAR


def BBI(DF, N1, N2, N3, N4):  # 多空指标
    C = DF['close']
    bbi = (MA(C, N1) + MA(C, N2) + MA(C, N3) + MA(C, N4)) / 4
    DICT = {'BBI': bbi}
    VAR = pd.DataFrame(DICT)
    return VAR


def BBIBOLL(DF, N1, N2, N3, N4, N, M):  # 多空布林线
    bbiboll = BBI(DF, N1, N2, N3, N4)
    UPER = bbiboll + M * STD(bbiboll, N)
    DOWN = bbiboll - M * STD(bbiboll, N)
    DICT = {'BBIBOLL': bbiboll, 'UPER': UPER, 'DOWN': DOWN}
    VAR = pd.DataFrame(DICT)
    return VAR


def PBX(DF, N1, N2, N3, N4, N5, N6):  # 瀑布线
    C = DF['close']
    PBX1 = (EMA(C, N1) + EMA(C, 2 * N1) + EMA(C, 4 * N1)) / 3
    PBX2 = (EMA(C, N2) + EMA(C, 2 * N2) + EMA(C, 4 * N2)) / 3
    PBX3 = (EMA(C, N3) + EMA(C, 2 * N3) + EMA(C, 4 * N3)) / 3
    PBX4 = (EMA(C, N4) + EMA(C, 2 * N4) + EMA(C, 4 * N4)) / 3
    PBX5 = (EMA(C, N5) + EMA(C, 2 * N5) + EMA(C, 4 * N5)) / 3
    PBX6 = (EMA(C, N6) + EMA(C, 2 * N6) + EMA(C, 4 * N6)) / 3
    DICT = {'PBX1': PBX1, 'PBX2': PBX2, 'PBX3': PBX3,
            'PBX4': PBX4, 'PBX5': PBX5, 'PBX6': PBX6}
    VAR = pd.DataFrame(DICT)
    return VAR


def BOLL(DF, N):  # 布林线
    C = DF['close']
    boll = MA(C, N)
    UB = boll + 2 * STD(C, N)
    LB = boll - 2 * STD(C, N)
    DICT = {'BOLL': boll, 'UB': UB, 'LB': LB}
    VAR = pd.DataFrame(DICT)
    return VAR


def ROC(DF, N, M):  # 变动率指标
    C = DF['close']
    roc = 100 * (C - REF(C, N)) / REF(C, N)
    MAROC = MA(roc, M)
    DICT = {'ROC': roc, 'MAROC': MAROC}
    VAR = pd.DataFrame(DICT)
    return VAR


def MTM(DF, N, M):  # 动量线
    C = DF['close']
    mtm = C - REF(C, N)
    MTMMA = MA(mtm, M)
    DICT = {'MTM': mtm, 'MTMMA': MTMMA}
    VAR = pd.DataFrame(DICT)
    return VAR


def MFI(DF, N):  # 资金指标
    C = DF['close']
    H = DF['high']
    L = DF['low']
    VOL = DF['vol']
    TYP = (C + H + L) / 3
    V1 = SUM(IF(TYP > REF(TYP, 1), TYP * VOL, 0), N) / \
         SUM(IF(TYP < REF(TYP, 1), TYP * VOL, 0), N)
    mfi = 100 - (100 / (1 + V1))
    DICT = {'MFI': mfi}
    VAR = pd.DataFrame(DICT)
    return VAR


def SKDJ(DF, N, M):
    """
    SKDJ 指标 (慢速随机指标)：通过差值平均过滤SKDJ指标的短期的波动，相比 KDJ 指标更可靠， SLOWKD参数通常设置为（36，5）
    KDJ指标： 研究一段时间内的，最高价、最低价和收盘价之间的关系， K值变动会比D值快，所以K值又称快线，D值又称慢线
    1.指标>80 时，回档机率大；指标<20 时，反弹机率大；
    2.K在20左右向上交叉D时，视为买进信号；
    3.K在80左右向下交叉D时，视为卖出信号；
    4.SKDJ波动于50左右的任何讯号，其作用不大。
    """
    CLOSE = DF['close']
    LOWV = LLV(DF['low'], N)
    HIGHV = HHV(DF['high'], N)
    RSV = EMA((CLOSE - LOWV) / (HIGHV - LOWV) * 100, M)
    K = EMA(RSV, M)
    D = MA(K, M)
    DICT = {'SKDJ_K': K, 'SKDJ_D': D}
    VAR = pd.DataFrame(DICT)
    return VAR


def WR(DF, N, N1):  # 威廉指标
    HIGH = DF['high']
    LOW = DF['low']
    CLOSE = DF['close']
    WR1 = 100 * (HHV(HIGH, N) - CLOSE) / (HHV(HIGH, N) - LLV(LOW, N))
    WR2 = 100 * (HHV(HIGH, N1) - CLOSE) / (HHV(HIGH, N1) - LLV(LOW, N1))
    DICT = {'WR1': WR1, 'WR2': WR2}
    VAR = pd.DataFrame(DICT)
    return VAR


def BIAS(DF, N1, N2, N3):  # 乖离率
    CLOSE = DF['close']
    BIAS1 = (CLOSE - MA(CLOSE, N1)) / MA(CLOSE, N1) * 100
    BIAS2 = (CLOSE - MA(CLOSE, N2)) / MA(CLOSE, N2) * 100
    BIAS3 = (CLOSE - MA(CLOSE, N3)) / MA(CLOSE, N3) * 100
    DICT = {'BIAS1': BIAS1, 'BIAS2': BIAS2, 'BIAS3': BIAS3}
    VAR = pd.DataFrame(DICT)
    return VAR


def RSI(DF, N1, N2, N3):  # 相对强弱指标RSI1:SMA(MAX(CLOSE-LC,0),N1,1)/SMA(ABS(CLOSE-LC),N1,1)*100;
    CLOSE = DF['close']
    LC = REF(CLOSE, 1)
    RSI1 = SMA(MAX(CLOSE - LC, 0), N1, 1) / SMA(ABS(CLOSE - LC), N1, 1) * 100
    RSI2 = SMA(MAX(CLOSE - LC, 0), N2, 1) / SMA(ABS(CLOSE - LC), N2, 1) * 100
    RSI3 = SMA(MAX(CLOSE - LC, 0), N3, 1) / SMA(ABS(CLOSE - LC), N3, 1) * 100
    DICT = {'RSI1': RSI1, 'RSI2': RSI2, 'RSI3': RSI3}
    VAR = pd.DataFrame(DICT)
    return VAR


def ADTM(DF, N, M):  # 动态买卖气指标
    HIGH = DF['high']
    LOW = DF['low']
    OPEN = DF['open']
    DTM = IF(OPEN <= REF(OPEN, 1), 0, MAX(
        (HIGH - OPEN), (OPEN - REF(OPEN, 1))))
    DBM = IF(OPEN >= REF(OPEN, 1), 0, MAX((OPEN - LOW), (OPEN - REF(OPEN, 1))))
    STM = SUM(DTM, N)
    SBM = SUM(DBM, N)
    ADTM1 = IF(STM > SBM, (STM - SBM) / STM,
               IF(STM == SBM, 0, (STM - SBM) / SBM))
    MAADTM = MA(ADTM1, M)
    DICT = {'ADTM': ADTM1, 'MAADTM': MAADTM}
    VAR = pd.DataFrame(DICT)
    return VAR


def DDI(DF, N, N1, M, M1):  # 方向标准离差指数
    H = DF['high']
    L = DF['low']
    DMZ = IF((H + L) <= (REF(H, 1) + REF(L, 1)), 0,
             MAX(ABS(H - REF(H, 1)), ABS(L - REF(L, 1))))
    DMF = IF((H + L) >= (REF(H, 1) + REF(L, 1)), 0,
             MAX(ABS(H - REF(H, 1)), ABS(L - REF(L, 1))))
    DIZ = SUM(DMZ, N) / (SUM(DMZ, N) + SUM(DMF, N))
    DIF = SUM(DMF, N) / (SUM(DMF, N) + SUM(DMZ, N))
    ddi = DIZ - DIF
    ADDI = SMA(ddi, N1, M)
    AD = MA(ADDI, M1)
    DICT = {'DDI': ddi, 'ADDI': ADDI, 'AD': AD}
    VAR = pd.DataFrame(DICT)
    return VAR


def DMA(DF, N1, N2, M):  # 平均差(不同 N 日均线的差值)
    CLOSE = DF['close']
    DIF = MA(CLOSE, N1) - MA(CLOSE, N2);
    DIFMA = MA(DIF, M);
    DICT = {'DIF': DIF, 'DIFMA': DIFMA}
    VAR = pd.DataFrame(DICT)
    return VAR


def DIF_AVG(DF, N1, N2):
    """
    基于成交均价 AVG 计算平均差, 使用 MA 不加权
    """
    AVG = DF['avg']
    DIF = MA(AVG, N1) - MA(AVG, N2)
    DICT = {'DIF_AVG': DIF}
    VAR = pd.DataFrame(DICT)
    return VAR


def DIF_AVG_E(DF, N1, N2):
    """
    基于成交均价 AVG 计算平均差,  短线 N1 使用 MEA 加权， 长线 N2 使用 MA 不加权
    """
    AVG = DF['avg']
    DIF = EMA(AVG, N1) - MA(AVG, N2)
    DICT = {'DIF_AVG_E': DIF}
    VAR = pd.DataFrame(DICT)
    return VAR
