import vectorbt as vbt
import pandas as pd
import numpy as np
from numba import njit

big_ts = pd.DataFrame(np.random.uniform(size=(1000, 1000)))

# pandas
@njit
def zscore_nb(x):
    return (x[-1] - np.mean(x)) / np.std(x)


big_ts.rolling(2).apply(zscore_nb, raw=True)
# %timeit big_ts.rolling(2).apply(zscore_nb, raw=True)


# 33.1 ms ± 1.17 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)


from datetime import datetime, timedelta

index = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(10)]
sr = pd.Series(np.arange(len(index)), index=index)
sr.vbt.rolling_split(
    window_len=5,
    set_lens=(1, 1),
    left_to_right=False,
    plot=True,
    trace_names=["train", "valid", "test"],
)
