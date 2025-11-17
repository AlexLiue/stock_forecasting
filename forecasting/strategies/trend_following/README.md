# 趋势跟踪策略 

趋势跟踪策略不预测拐点，而是等待趋势确立后顺势进入。目标是在趋势中段赚取利润，而非猜底或抄顶。

## 1️⃣ 移动均线交叉策略 (MA Crossover)  
思路：短期均线反映当前动量；长期均线反映趋势方向。  
信号：买入：MA_short 上穿 MA_long； 卖出：MA_short 下穿 MA_long。
参数示例：20日MA 与 60日MA。  
优点：简单稳健、长期有效；  
缺点：震荡市假信号多。  

## 2️⃣ 动量策略 (Momentum / Time-Series Momentum)   
判断标的在过去一段时间收益是否延续。  
信号： Momentum > 0 → 做多；Momentum < 0 → 做空；   
广泛用于： 股票横截面动量； 期货时间序列动量（AQR “Dual Momentum” 策略）。  

## 3️⃣ 突破策略 (Price Channel / Donchian Channel)
思路：价格突破一定区间的极值后往往延续。
信号： 价格突破过去 N 日最高价 → 做多； 价格跌破过去 N 日最低价 → 做空。
Donchian 通道（Richard Donchian）是早期期货趋势策略鼻祖。
常用参数：N = 20, 55

## 4️⃣ 趋势过滤 + 动态止损策略 (ADX / ATR)
结合趋势强度判断与波动自适应风控：  
ADX（Average Directional Index）：趋势强度过滤；  
ATR（Average True Range）：用于动态止损或调仓；  
在趋势市场保持仓位，在震荡中减少交易频率。  

## 5️⃣ Kalman 滤波趋势线    
用状态空间模型实时估计“隐含趋势线”，对噪音有鲁棒性；  
比均线更平滑且反应更快；  
更适合程序化短期趋势追踪。  



