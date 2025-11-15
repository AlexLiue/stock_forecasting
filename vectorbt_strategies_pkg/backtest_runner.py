import os, yfinance as yf, matplotlib.pyplot as plt
from .strategies_core import *

def run_strategy(factory, name, close, **kwargs):
    entries, exits = factory.run(close, **kwargs).entries, factory.run(close, **kwargs).exits
    pf = vbt.Portfolio.from_signals(close, entries, exits, init_cash=10000)
    plt.figure(); pf.value().plot(title=f"{name} 累计收益曲线")
    plt.xlabel("日期"); plt.ylabel("资产价值"); plt.tight_layout()
    plt.savefig(f"charts/{name}.png"); plt.close()
    return pf.total_return()

def batch_backtest(symbol="AAPL"):
    os.makedirs("charts", exist_ok=True)
    data = yf.download(symbol, start="2018-01-01", progress=False)
    close = data["Close"]
    strategies = {
        "MA_Cross": (MA_Cross, {}),
        "RSI_MeanReversion": (RSI_MeanReversion, {}),
    }
    results = {}
    for name, (fac, params) in strategies.items():
        print(f"🔹 执行策略 {name}")
        try:
            results[name] = float(run_strategy(fac, name, close, **params))
        except Exception as e:
            print(f"❌ {name} 出错: {e}")
    df = pd.DataFrame.from_dict(results, orient="index", columns=["total_return"])
    df.to_csv("strategy_results_summary.csv")
    print("✅ 所有策略完成，结果已保存。")
    return df


