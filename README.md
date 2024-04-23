# 股票分析预测

## 数据源 [Tushare](https://tushare.pro)


数据同步拉取: [https://github.com/AlexLiue/stock_forecasting/tree/master/tushare_sync](https://github.com/AlexLiue/stock_forecasting/tree/master/tushare_sync)   
附加特征计算：[https://github.com/AlexLiue/stock_forecasting/tree/master/features_calculate](https://github.com/AlexLiue/stock_forecasting/tree/master/features_calculate)   
股票预测分析：[https://github.com/AlexLiue/stock_forecasting/tree/master/forecasting](https://github.com/AlexLiue/stock_forecasting/tree/master/forecasting)    

## 项目文件说明
### Python 包结构说明

| 项目包名          | 包路径                            | 包说明                |  
|:--------------|:-------------------------------|:-------------------|  
| tushare_sync  | [tushare_sync](tushare_sync)   | 原始 MySQL 数据读取以及粗加工 |  
| features_calculate | [features_cash](features_calculate) | 特征加工处理             |  
| forecasting   | [forecasting](forecasting)     | 根据特征信息进行模型预测       |  


## 使用手册

### 配置 Python 环境
Python 版本 >= 3.8.x ， 安装依赖包 [requirements.txt](requirements.txt)
```shell
# 股票源
conda install -c conda-forge quandl
pip install tushare --upgrade
conda install -c conda-forge pytrends
#  基于 Python 安装
python -m pip install TA-Lib
# 时间序分析包 
conda install -c conda-forge prophet

```

### 配置MySQL 
```shell
CREATE USER 'stock'@'%' IDENTIFIED BY 'stock';
CREATE DATABASE stock;
GRANT ALL PRIVILEGES ON stock.* TO 'stock'@'%';
ALTER USER 'stock'@'%' IDENTIFIED WITH mysql_native_password BY 'stock';
FLUSH PRIVILEGES;
```






