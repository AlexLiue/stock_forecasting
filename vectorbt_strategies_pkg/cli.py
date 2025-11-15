import argparse, os, yfinance as yf
from .backtest_runner import batch_backtest
from .optimization import optimize_ma_cross, compare_strategies
from .visualization import plot_radar, export_pdf_report

def main():
    parser=argparse.ArgumentParser(description="VectorBT 策略合集命令行工具")
    parser.add_argument("mode",choices=["backtest","optimize","compare","radar","report"],help="运行模式")
    parser.add_argument("--symbol",default="AAPL",help="股票代码")
    args=parser.parse_args()

    os.makedirs("charts", exist_ok=True)
    data=yf.download(args.symbol,start="2018-01-01",progress=False)
    close=data["Close"]

    if args.mode=="backtest":
        batch_backtest(args.symbol)
    elif args.mode=="optimize":
        optimize_ma_cross(close)
    elif args.mode=="compare":
        compare_strategies(close)
    elif args.mode=="radar":
        plot_radar()
    elif args.mode=="report":
        export_pdf_report()
    else:
        print("❌ 未知模式")

if __name__=="__main__":
    main()

    