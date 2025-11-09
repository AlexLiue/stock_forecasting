from datetime import timedelta

import pandas as pd
from pandas import Timestamp


def get_split_range(start_date, end_date, freq="70D"):
    """
    将时间拆分成若干个区间, 单次执行一个区间的数据同步, 防止单次拉取数据量过大
    """
    # 转换为 Timestamp
    start = pd.to_datetime(start_date, format="%Y%m%d")
    end: Timestamp = pd.to_datetime(end_date, format="%Y%m%d")

    # 生成每10周一个的区间边界
    intervals = pd.date_range(start=start, end=end, freq=freq)

    # 将拆分的子区间组织成 (start, end)
    result = []
    for i in range(len(intervals) - 1):
        result.append(
            (
                intervals[i].date().strftime("%Y%m%d"),
                (intervals[i + 1] - timedelta(days=1)).date().strftime("%Y%m%d"),
            )
        )

    # 最后一个区间（如果不是精确划分）
    if intervals[-1] <= end:
        result.append(
            (intervals[-1].date().strftime("%Y%m%d"), end.date().strftime("%Y%m%d"))
        )
    return result


# 示例输入
start_date = "20240105"
end_date = "20241105"
res = get_split_range(start_date, end_date)

# 展示结果
for i, (s, e) in enumerate(res, 1):
    print(f"子区间 {i}: {s} → {e}")

#
#
# # 转换为 Timestamp
# start = pd.to_datetime(start_date)
# end = pd.to_datetime(end_date)
#
# # 生成每10周一个的区间边界
# intervals = pd.date_range(start=start, end=end, freq='70D')
#
# # 将拆分的子区间组织成 (start, end)
# result = []
# for i in range(len(intervals) - 1):
#     result.append((intervals[i].date(), (intervals[i+1] - timedelta(days=1)).date()))
#
# # 最后一个区间（如果不是精确划分）
# if intervals[-1] < end:
#     result.append((intervals[-1].date(), end.date()))
#
#
#
#
#
# # 展示结果
# for i, (s, e) in enumerate(result, 1):
#     print(f"子区间 {i}: {s} → {e}")
