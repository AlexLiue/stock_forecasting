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

### 配置 Oracle 环境  
#### Oracle 数据库环境准备  
考虑查询计算性能, 采用 Oracle 数据库进行存储, Docker 模式安装 Oracle 19C, 并创建数据库表空间和用户  
```
## Oracle 数据库安装
### Windows 平台 X86
docker run -d --name ORACLE19C -p 11521:1521 -p 15500:5500 -e ORACLE_SID=STOCK -e ORACLE_PDB=STOCK1 -e ORACLE_PWD=Stock#123 -e ORACLE_EDITION=standard -e ORACLE_CHARACTERSET=AL32UTF8 -v oracle_data:/opt/oracle/oradata registry.cn-hangzhou.aliyuncs.com/laowu/oracle:19c


### MacOS 平台 ARM64
docker run --name ORACLE19C -d -p 11521:1521 -p 15500:5500 -p 12484:2484 -e ORACLE_SID=STOCK -e ORACLE_PDB=STOCK1 -e ORACLE_PWD=Oracle#123 -v oracle_data:/opt/oracle/oradata codeassertion/oracledb-arm64-standalone:19.3.0-enterprise

## 创建数据库
CREATE TABLESPACE  akshare DATAFILE '/opt/oracle/oradata/STOCK/STOCK1/akshare.dbf' SIZE 4G AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED;
alter session set "_ORACLE_SCRIPT"=true;
CREATE USER akshare IDENTIFIED BY Akshare009  DEFAULT TABLESPACE akshare;
GRANT ALL PRIVILEGES TO akshare;
```
####  修改配置文件信息(本地数据库地址信息)   
编辑 application.ini 修改本地数据库的地址用户密码  
```

[oracle]
host=localhost
port=11521
user=akshare
password=Akshare009
service_name=STOCK
client_win=C:\Apps\OracleClient\instantclient_19_28
client_macos=/opt/instantclient_23_3








