#
# #############################################
# #     📈 示例：MA_Cross 策略参数优化可视化
# #############################################
#
# import itertools
# import seaborn as sns
# import vectorbt as vbt
#
# from vectorbt_strategies_pkg.vectorbt_strategies_full import MA_Cross
#
#
# def optimize_ma_cross(short_range=range(5, 21, 5),
#                       long_range=range(30, 101, 10)):
#     """自动测试不同MA组合参数并绘制热力图"""
#     print("🧠 开始 MA_Cross 参数网格搜索...")
#     results = []
#
#     for short_window, long_window in itertools.product(short_range, long_range):
#         if short_window >= long_window:
#             continue
#         ma_cross = MA_Cross.run(close,
#                                 short_window=short_window,
#                                 long_window=long_window)
#         pf = vbt.Portfolio.from_signals(close,
#                                         entries=ma_cross.entries,
#                                         exits=ma_cross.exits,
#                                         init_cash=10000)
#         total = pf.total_return()
#         results.append((short_window, long_window, total))
#
#     df = pd.DataFrame(results, columns=['short_window', 'long_window', 'return'])
#     pivot = df.pivot(index='short_window', columns='long_window', values='return')
#
#     plt.figure(figsize=(8, 6))
#     sns.heatmap(pivot, cmap="YlGnBu", annot=True, fmt=".2f")
#     plt.title("MA_Cross 参数优化热力图（总收益率）")
#     plt.xlabel("长周期窗口")
#     plt.ylabel("短周期窗口")
#     plt.tight_layout()
#     plt.savefig("charts/MA_Cross_param_heatmap.png")
#     plt.close()
#     print("✅ 参数热力图已保存至 charts/MA_Cross_param_heatmap.png")
#
#     best = df.loc[df['return'].idxmax()]
#     print(f"🌟 最优参数组合: short={best.short_window}, long={best.long_window}, 收益={best.return:.2%}")
#     return df
#
# # 运行优化函数
# if __name__ == "__main__":
#     optimize_ma_cross()
