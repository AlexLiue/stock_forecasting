
# 📈 VectorBT Strategies Package

![CI](https://github.com/yourusername/your-repo/actions/workflows/ci.yml/badge.svg)
![Python Version](https://img.shields.io/badge/python-3.9--3.11-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 🧭 项目简介

**VectorBT Strategies Package** 是一个模块化的 Python 量化研究框架，  
集成了 50 + 常见交易策略（趋势追踪、均值回归、动量、突破、形态等），  
并支持 **参数优化、策略回测、可视化图表、雷达图对比、PDF 报告导出、命令行工具与自动化测试**。

基于 [`vectorbt`](https://vectorbt.dev) 开发，适用于策略研究、教学与自动化策略评估。

---

## ⚙️ 安装与依赖

```bash
git clone https://github.com/yourusername/your-repo.git
cd your-repo
pip install -e .
```

**主要依赖：**
- vectorbt  
- yfinance  
- numpy, pandas, matplotlib, seaborn  
- fpdf  
- pytest (测试用)

---

## 🚀 快速开始

运行命令行工具：

```bash
# 运行批量回测
vbt-strategies backtest --symbol AAPL

# 参数优化 (MA_Cross)
vbt-strategies optimize

# 多策略对比
vbt-strategies compare

# 雷达图
vbt-strategies radar

# 导出PDF报告
vbt-strategies report
```

所有图表与报告都会自动保存在 `charts/` 文件夹。

---

## 🧪 测试

运行全部 pytest 单元测试：

```bash
pytest -v
```

### ✅ GitHub Actions 自动化（CI）

本项目内置 `.github/workflows/ci.yml`，  
每次 push/PR 都会触发以下自动测试：

- 安装依赖  
- 运行 pytest  
- 上传结果

---

## 📊 功能展示

| 可视化类型 | 输出文件 | 说明 |
|-------------|-----------|------|
| 策略收益曲线 | `charts/<策略名>.png` | 单策略回测收益走势 |
| 参数热力图 | `charts/MA_Cross_heatmap.png` | 参数优化结果 |
| 多策略对比线 | `charts/strategy_compare.png` | 参数敏感性对比 |
| 雷达分析图 | `charts/strategy_radar_chart.png` | 整体策略表现 |
| 自动报告 | `Strategy_Report.pdf` | 汇总可分享报告 |

---

## 📂 项目结构

```
vectorbt_strategies_pkg/
├── __init__.py
├── strategies_core.py
├── backtest_runner.py
├── optimization.py
├── visualization.py
├── cli.py
├── setup.py
├── tests/
│   ├── test_strategies_basic.py
│   ├── test_backtest_runner.py
│   ├── test_visualization_report.py
└── .github/workflows/ci.yml
```

---

## 🧩 扩展与开发

- 在 `strategies_core.py` 中添加新策略即可自动注册  
- 自定义参数区间以扩展敏感性分析  
- 通过 `pytest` 添加单元测试保证新策略安全  
- 可在 GitHub Actions 工作流中拓展性能评估步骤（Backtest Speed、Sharpe Ratio 分析等）

---

## 🪪 License

本项目采用 **MIT License**，可自由使用、修改与发布。

---
🧠 *Created with AI-assisted Quant Optimization Framework – 2025 Edition by QuantAI.*
