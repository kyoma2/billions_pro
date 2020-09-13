#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd


def EMA(DF, N):
    return pd.Series.ewm(DF, span=N, min_periods=N - 1, adjust=True).mean()


def MA(DF, N):
    DF = DF.sort_values(by= ['date'])
    return pd.Series.rolling(DF['close'], N).mean()


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


def MACD(DF, FAST, SLOW, MID):

    # DF = DF.sort_values(by= ['date'],ascending=False)
    EMAFAST = EMA(DF['close'], FAST)
    EMASLOW = EMA(DF['close'], SLOW)
    DIFF = EMAFAST - EMASLOW
    DEA = EMA(DIFF, MID)
    MACD = (DIFF - DEA) * 2
    DICT = {'DIFF': DIFF, 'DEA': DEA, 'MACD': MACD}
    VAR = pd.DataFrame(DICT)
    #
    # VAR['C'] = range(len(VAR))
    # VAR = VAR.sort_values(by=['C'],ascending=False)
    return VAR['DIFF'].tolist(),VAR['DEA'].tolist(),VAR['MACD'].tolist()


def KDJ(DF, N, M1, M2):
    DF =DF.sort_values(by= ['date'],ascending=True)
    C = DF['close']
    H = DF['high']
    L = DF['low']
    RSV = (C - LLV(L, N)) / (HHV(H, N) - LLV(L, N)) * 100
    K = SMA(RSV, M1, 1)
    D = SMA(K, M2, 1)
    J = 3 * K - 2 * D
    DICT = {'KDJ_K': K, 'KDJ_D': D, 'KDJ_J': J}
    VAR = pd.DataFrame(DICT)

    return VAR['KDJ_K'],VAR['KDJ_D'],VAR['KDJ_J']


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


def ma(data, n=10, val_name="close"):
    import numpy as np

    '''
    移动平均线 Moving Average
    Parameters
    ------
      data:pandas.DataFrame
                  通过 get_h_data 取得的股票数据
      n:int
                  移动平均线时长，时间单位根据data决定
      val_name:string
                  计算哪一列的列名，默认为 close 收盘值

    return
    -------
      list
          移动平均线
    '''

    values = []
    MA = []

    for index, row in data.iterrows():
        values.append(row[val_name])
        if len(values) == n:
            del values[0]

        MA.append(np.average(values))

    return np.asarray(MA)




def ema(data, n=12, val_name="close"):
    import numpy as np
    '''
        指数平均数指标 Exponential Moving Average
        Parameters
        ------
          data:pandas.DataFrame
                      通过 get_h_data 取得的股票数据
          n:int
                      移动平均线时长，时间单位根据data决定
          val_name:string
                      计算哪一列的列名，默认为 close 收盘值

        return
        -------
          EMA:numpy.ndarray<numpy.float64>
              指数平均数指标
    '''

    prices = []

    EMA = []

    for index, row in data.iterrows():
        if index == 0:
            past_ema = row[val_name]
            EMA.append(row[val_name])
        else:
            # Y=[2*X+(N-1)*Y’]/(N+1)
            today_ema = (2 * row[val_name] + (n - 1) * past_ema) / (n + 1)
            past_ema = today_ema

            EMA.append(today_ema)

    return np.asarray(EMA)
