# 股票分析预测

## 数据源 [Akshare](https://akshare.akfamily.xyz/introduction.html)


数据同步拉取: [https://github.com/AlexLiue/stock_forecasting/tree/master/akshare_sync](https://github.com/AlexLiue/stock_forecasting/tree/master/akshare_sync)   
股票预测分析：[https://github.com/AlexLiue/stock_forecasting/tree/master/forecasting](https://github.com/AlexLiue/stock_forecasting/tree/master/forecasting)    

## 项目文件说明
### Python 包结构说明

| 项目包名          | 包路径                            | 包说明                |  
|:--------------|:-------------------------------|:-------------------|  
| akshare_sync  | [akshare_sync](akshare_sync)   | 原始 MySQL 数据读取以及粗加工 |
| forecasting   | [forecasting](forecasting)     | 根据特征信息进行模型预测       |  


## 使用手册

### 配置 Python 环境
Python 版本 >= 3.8.x ， 安装依赖包 [requirements.txt](requirements.txt)
```shell
# 股票源
pip install akshare --upgrade
```

### 配置MySQL 
```shell
CREATE USER 'stock'@'%' IDENTIFIED BY 'stock';
CREATE DATABASE stock;
GRANT ALL PRIVILEGES ON stock.* TO 'stock'@'%';
ALTER USER 'stock'@'%' IDENTIFIED WITH mysql_native_password BY 'stock';
FLUSH PRIVILEGES;
```






