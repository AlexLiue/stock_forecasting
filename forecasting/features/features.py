"""
特征值计算

"""

import sys

from forecasting.features.stock import Stock


if __name__ == "__main__":
    stock = Stock(
        period="day", symbol="000001", start_date="20210101", end_date="20240501"
    )
    stock.plot()

    stock = Stock(
        period="min30", symbol="000001", start_date="20210101", end_date="20240501"
    )
    stock.plot()

    sys.exit(0)
