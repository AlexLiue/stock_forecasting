import itertools, seaborn as sns
import matplotlib.pyplot as plt, pandas as pd, numpy as np
import vectorbt as vbt, os
from strategies_core import MA_Cross, RSI_MeanReversion

def optimize_ma_cross(close, short_range=range(5,21,5), long_range=range(30,101,10)):
    results=[]
    os.makedirs("charts", exist_ok=True)
    for s,l in itertools.product(short_range,long_range):
        if s>=l: continue
        ma=MA_Cross.run(close, short_window=s, long_window=l)
        pf=vbt.Portfolio.from_signals(close, ma.entries, ma.exits, init_cash=10000)
        results.append((s,l,float(pf.total_return())))
    df=pd.DataFrame(results,columns=["short","long","return"])
    pivot=df.pivot("short","long","return")
    plt.figure(figsize=(8,6))
    sns.heatmap(pivot, annot=True, cmap="YlGnBu", fmt=".2f")
    plt.title("MA_Cross 策略参数热力图")
    plt.savefig("charts/MA_Cross_heatmap.png"); plt.close()
    best = df.loc[df["return"].idxmax()]
    print(f"🌟 最优参数: short={best.short}, long={best.long}, 收益={best['return']:.2%}")
    return df

def compare_strategies(close):
    print("📊 多策略参数对比...")
    os.makedirs("charts", exist_ok=True)
    curves={}
    ranges={
        "MA_Cross": range(5,30,5),
        "RSI_MeanReversion": range(5,30,5)
    }
    plt.figure(figsize=(9,6))
    for name,vals in ranges.items():
        rets=[]
        fac=MA_Cross if name=="MA_Cross" else RSI_MeanReversion
        for v in vals:
            try:
                ind=fac.run(close, window=v)
                pf=vbt.Portfolio.from_signals(close, ind.entries, ind.exits, init_cash=10000)
                rets.append(float(pf.total_return()))
            except: rets.append(np.nan)
        curves[name]=rets
        plt.plot(vals,rets,marker="o",label=name)
    plt.legend(); plt.title("多策略参数敏感性折线图")
    plt.xlabel("参数窗口"); plt.ylabel("总收益率")
    plt.tight_layout(); plt.savefig("charts/strategy_compare.png"); plt.close()
    pd.DataFrame(curves,index=list(ranges["MA_Cross"])).to_csv("multi_strategy_param_comparison.csv")
    print("✅ 对比完成。")

