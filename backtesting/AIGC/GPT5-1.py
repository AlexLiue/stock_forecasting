

import pandas as pd
import numpy as np
import lightgbm as lgb
import yfinance as yf
import matplotlib.pyplot as plt
import seaborn as sns
import shap
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error
from bayes_opt import BayesianOptimization
from fpdf import FPDF
import os

plt.rcParams['font.sans-serif'] = ['LiSu Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

import os

# 设置系统级环境变量
os.environ['HTTP_PROXY'] = 'http://d66b64c8435e81c4ce2b__cr.us:280a4e739fb93510@proxy.cheapproxy.net:823'
os.environ['HTTPS_PROXY'] = 'http://d66b64c8435e81c4ce2b__cr.us:280a4e739fb93510@proxy.cheapproxy.net:823'


class QuantModel:
    def __init__(self, ticker="1810.HK", start="2020-01-01", end="2025-11-14"):
        self.ticker = ticker
        self.data = yf.download(ticker, start=start, end=end)
        self.hsi = yf.download("^HSI", start=start, end=end)
        self.model = None
        self.results = {}
        self.end = end

    def prepare_data(self):
        df = self.data.copy()
        df.columns = df.columns.get_level_values(0)
        df["Return_5d"] = df["Close"].pct_change(5)
        df["MA20"] = df["Close"].rolling(20).mean()
        df["MA60"] = df["Close"].rolling(60).mean()
        df["RSI"] = 100 - (100 / (1 + df["Return_5d"].rolling(14).mean()))
        df["Volatility"] = df["Return_5d"].rolling(20).std()
        df["Target_60d"] = df["Close"].shift(-60)
        df.dropna(inplace=True)

        self.features = ["Return_5d", "MA20", "MA60", "RSI", "Volatility"]
        X, y = df[self.features], df["Target_60d"]
        split_point = int(len(df) * 0.8)
        self.X_train, self.X_test = X.iloc[:split_point], X.iloc[split_point:]
        self.y_train, self.y_test = y.iloc[:split_point], y.iloc[split_point:]
        self.scaler = StandardScaler()
        self.X_train_scaled = self.scaler.fit_transform(self.X_train)
        self.X_test_scaled = self.scaler.transform(self.X_test)
        self.df_test = df.iloc[split_point:]
        print(f"✅ 数据准备完成 | 训练样本: {len(self.X_train)}, 测试样本: {len(self.X_test)}")

    def optimize_params(self):
        def lgb_cv(num_leaves, feature_fraction, bagging_fraction, max_depth, min_data_in_leaf, learning_rate):
            model = lgb.LGBMRegressor(
                n_estimators=400,
                num_leaves=int(num_leaves),
                feature_fraction=feature_fraction,
                bagging_fraction=bagging_fraction,
                max_depth=int(max_depth),
                min_data_in_leaf=int(min_data_in_leaf),
                learning_rate=learning_rate,
                random_state=42
            )
            model.fit(self.X_train_scaled, self.y_train)
            y_pred = model.predict(self.X_test_scaled)
            return -mean_absolute_error(self.y_test, y_pred)

        pbounds = {
            "num_leaves": (15, 60),
            "feature_fraction": (0.5, 1),
            "bagging_fraction": (0.5, 1),
            "max_depth": (3, 10),
            "min_data_in_leaf": (10, 60),
            "learning_rate": (0.005, 0.05)
        }

        optimizer = BayesianOptimization(f=lgb_cv, pbounds=pbounds, random_state=42, verbose=0)
        optimizer.maximize(init_points=5, n_iter=8)
        self.best_params = optimizer.max["params"]
        print("🏆 最优参数:", self.best_params)

    def train(self):
        bp = self.best_params
        self.model = lgb.LGBMRegressor(
            n_estimators=600,
            num_leaves=int(bp["num_leaves"]),
            feature_fraction=bp["feature_fraction"],
            bagging_fraction=bp["bagging_fraction"],
            max_depth=int(bp["max_depth"]),
            min_data_in_leaf=int(bp["min_data_in_leaf"]),
            learning_rate=bp["learning_rate"],
            random_state=42
        )
        self.model.fit(self.X_train_scaled, self.y_train)
        self.y_pred = self.model.predict(self.X_test_scaled)
        print("✅ 模型训练完成")

    def explain_model(self, plot_dir="plots"):
        os.makedirs(plot_dir, exist_ok=True)

        # 特征重要性图
        importance = pd.Series(self.model.feature_importances_, index=self.features).sort_values()
        plt.figure(figsize=(6,4))
        importance.plot(kind="barh", color="teal")
        plt.title("特征重要性")
        plt.tight_layout()
        plt.savefig(f"{plot_dir}/importance.png")
        plt.close()

        # SHAP分析
        explainer = shap.TreeExplainer(self.model)
        shap_values = explainer.shap_values(self.X_test_scaled)
        shap.summary_plot(shap_values, self.X_test, feature_names=self.features, show=False)
        plt.tight_layout()
        plt.savefig(f"{plot_dir}/shap_summary.png")
        plt.close()
        print("🧠 模型解释分析完成")

    def backtest(self):
        test_df = self.df_test.copy()
        test_df.columns = test_df.columns.get_level_values(0)
        test_df["Predicted"] = self.y_pred
        test_df["Return"] = test_df["Close"].pct_change()
        test_df["Signal"] = np.where(test_df["Predicted"] >= test_df["Close"] * 1.05, 1, 0)
        test_df["Position"] = test_df["Signal"].shift(1).fillna(0)
        test_df["Strategy"] = test_df["Position"] * test_df["Return"]
        test_df["Net_Strategy"] = test_df["Strategy"] - test_df["Position"].diff().abs() * 0.001

        cum_ret = (1 + test_df["Net_Strategy"]).cumprod() - 1
        hsi = self.hsi.loc[cum_ret.index]
        hsi_ret = (1 + hsi["Close"].pct_change().fillna(0)).cumprod() - 1

        self.results = {
            "annual_return": (1 + cum_ret.iloc[-1]) ** (252 / len(test_df)) - 1,
            "max_drawdown": (cum_ret - cum_ret.cummax()).min(),
            "sharpe": ((1 + cum_ret.iloc[-1]) ** (252 / len(test_df)) - 1 - 0.02) / (test_df["Net_Strategy"].std() * np.sqrt(252)),
            "cum_ret": cum_ret,
            "hsi_ret": hsi_ret
        }

        print(f"📊 年化收益率: {self.results['annual_return']*100:.2f}% | 夏普: {self.results['sharpe']:.2f} | 回撤: {self.results['max_drawdown']*100:.2f}%")

    def generate_plots(self, plot_dir="plots"):
        os.makedirs(plot_dir, exist_ok=True)
        cum_ret, hsi_ret = self.results['cum_ret'], self.results['hsi_ret']

        plt.figure(figsize=(8,5))
        plt.plot(cum_ret, label="策略累计收益", color="blue")
        plt.plot(hsi_ret, label="恒生指数收益", color="gray", linestyle="--")
        plt.title(f"{self.ticker} 策略 vs 恒指 表现对比")
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{plot_dir}/comparison.png")
        plt.close()

    def create_report(self, output_file="quant_report_explain.pdf", plot_dir="plots"):
        pdf = FPDF()
        pdf.add_page()
        pdf.add_font('LiSu', '', 'C:\Windows\Fonts\SIMLI.TTF', uni=True)
        pdf.set_font("LiSu", "", 16)
        pdf.cell(0, 10, f"{self.ticker} 量化回测与解释报告", ln=True)
        pdf.set_font("LiSu", size=12)
        pdf.cell(0, 10, f"年化收益率: {self.results['annual_return']*100:.2f}%", ln=True)
        pdf.cell(0, 10, f"最大回撤: {self.results['max_drawdown']*100:.2f}%", ln=True)
        pdf.cell(0, 10, f"夏普比率: {self.results['sharpe']:.2f}", ln=True)

        for img in ["comparison.png", "importance.png", "shap_summary.png"]:
            pdf.add_page()
            pdf.image(f"{plot_dir}/{img}", x=20, y=30, w=170)
        pdf.output(output_file)
        print(f"📄 解释型量化报告已生成: {output_file}")


if __name__ == "__main__":
    model = QuantModel("1810.HK")
    model.prepare_data()
    model.optimize_params()
    model.train()
    model.explain_model()
    model.backtest()
    model.generate_plots()
    model.create_report()
