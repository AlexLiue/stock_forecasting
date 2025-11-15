def calc_return(df, col='Close', period=1, new_col=None):
    """
    计算指定列的周期百分比变化率（收益率）

    参数：
        df (pd.DataFrame): 包含目标列的数据表
        col (str): 计算变化率的列名（默认 'Close'）
        period (int): 与前多少期对比，默认为 1（即每日收益率）
        new_col (str): 输出新列名（默认为 Return_{period}d）

    返回：
        pd.DataFrame: 添加新列后的 DataFrame
    """
    if new_col is None:
        new_col = f'Return_{period}d'

    if col not in df.columns:
        raise ValueError(f"列 '{col}' 不存在于 DataFrame 中")

    df[new_col] = df[col].pct_change(periods=period)
    return df


def calc_cum_return(df, price_col='Close', return_col='daily_return', cum_col='cum_return', group_col=None):
    """
    计算每日收益率与累计收益率

    参数：
        df (pd.DataFrame): 输入数据，需包含价格列
        price_col (str): 价格列名，如 'Close' 或 'Adj Close'
        return_col (str): 输出每日收益率列名
        cum_col (str): 输出累计收益率列名
        group_col (str): 若指定分组列（如 'Ticker'），则对每组分别计算

    返回：
        pd.DataFrame: 含收益率结果的新 DataFrame
    """

    if price_col not in df.columns:
        raise ValueError(f"列 '{price_col}' 不存在于 DataFrame 中")

    df = df.copy()

    if group_col:
        # 对每个分组（如多只股票）
        df[return_col] = df.groupby(group_col)[price_col].pct_change()
        df[cum_col] = df.groupby(group_col)[return_col].apply(lambda x: (1 + x).cumprod() - 1)
    else:
        # 单一序列
        df[return_col] = df[price_col].pct_change()
        df[cum_col] = (1 + df[return_col]).cumprod() - 1

    return df
