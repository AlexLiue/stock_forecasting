import datetime

import quandl
from prophet import Prophet
import pandas as pd
from matplotlib import pyplot as plt
import logging

from forecasting.utils.utils import load_table

logging.getLogger("prophet").setLevel(logging.ERROR)
import warnings

warnings.filterwarnings("ignore")

plt.rcParams["figure.figsize"] = 9, 6

ts_code = "000001.SZ"
start_date = "20200101"
end_date = "20240501"
daily = load_table("daily", ts_code=ts_code, start_date=start_date, end_date=end_date)

# daily['trade_date'] = daily['trade_date'].map(lambda date: str(date))

ds = daily["trade_date"].map(
    lambda date: datetime.datetime.strptime(str(date), "%Y%m%d")
)
y = (daily["amount"] * 10) / daily["vol"]

df = pd.DataFrame({"ds": ds, "y": y})
m = Prophet()

m = m.fit(df)
future = m.make_future_dataframe(periods=1000)
forecast = m.predict(future)

# Python
m.plot(forecast)
plt.axhline(y=0, color="red")
plt.title("Default Prophet")
