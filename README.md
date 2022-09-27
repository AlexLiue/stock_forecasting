# 股票分析预测

## 数据源 [Tushare](https://tushare.pro)

数据同步拉取工具: [https://github.com/AlexLiue/tushare_sync](https://github.com/AlexLiue/tushare_sync)

## 项目文件说明

### Python 包结构说明

| 项目包名          | 包路径                            | 包说明                |  
|:--------------|:-------------------------------|:-------------------|  
| datas         | [datas](datas)                 | 原始 MySQL 数据读取以及粗加工 |  
| features      | [features](features)           | 特征加工处理             |  
| features_cash | [features_cash](features_cash) | 特征与加工处理并存储 MySQL   |  

### Python 源文件说明

| 项目包名    | Python 源文件                          | 源文件说明          |    
|:--------|:------------------------------------|:---------------|    
| datas   | [tools.py](datas/tools.py)          | 数据读取-公共函数定义    |  
| datas   | [trande_cal.py](datas/trade_cal.py) | 数据读取-交易所交易日历信息 |    

CREATE USER 'testuser'@'%' IDENTIFIED BY 'testpasswd'; GRANT ALL PRIVILEGES ON test.* TO 'testuser'@'%' ; GRANT SELECT
ON stock.* TO 'testuser'@'%' ; FLUSH PRIVILEGES;

## 特征数据

### 3 日线 计算

为降低重复计算成本, 3日线计算结果,存储于 MySQL 数据库中

