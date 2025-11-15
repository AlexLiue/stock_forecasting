from setuptools import setup, find_packages

setup(
    name="vectorbt_strategies_pkg",
    version="1.0.0",
    author="QuantAI",
    description="基于 VectorBT 的多策略量化分析与可视化框架（含50+策略定义、优化、报告）",
    packages=find_packages(),
    install_requires=[
        "vectorbt","yfinance","matplotlib","numpy","pandas","seaborn","fpdf"
    ],
    entry_points={
        "console_scripts": [
            "vbt-strategies=vectorbt_strategies_pkg.cli:main"
        ]
    },
)
