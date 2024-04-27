# Sync Akshare Data to MySQL - 股票数据获取

- 同步 [Akshare](https://akshare.akfamily.xyz/data/stock/stock.html) 的股票交易数据到本地 MySQL 进行存储
- 首先从 Akshare 拉取全量历史数据
- 然后每日下午 从 Akshare 拉取当日增量数据
- 数据包含: A股、港股、日线、周线、月线等

## 使用方法
```shell
python data_sun.py
```

## MySQL 数据库环境准备

##### 创建数据库
```
docker network create --subnet=192.168.20.0/24 --gateway=192.168.20.1 docker_bridge
mkdir -p ~/Apps/Dockers/mysql/mysql_tushare/var/lib/mysql
mkdir -p ~/Apps/Dockers/mysql/mysql_tushare/etc/mysql/conf.d
docker run -itd --name mysql_akshare \
  -p 3306:3306 \
  -v ~/Apps/Dockers/mysql/mysql_akshare/var/lib/mysql:/var/lib/mysql \
  -v ~/Apps/Dockers/mysql/mysql_akshare/etc/mysql/conf.d:/etc/mysql/conf.d \
  -e MYSQL_ROOT_PASSWORD=akshare_root \
  -e MYSQL_DATABASE=akshare \
  -e MYSQL_USER=akshare \
  -e MYSQL_PASSWORD=alshare \
  --net docker_bridge \
  --ip 192.168.20.10 \
   mysql:8.0.32

# Other  
docker run -itd --name mysql_tushare   -p3310:3306   -v /Users/alex/Apps/Dockers/mysql/mysql_tushare/var/lib/mysql:/var/lib/mysql   -v /Users/alex/Apps/Dockers/mysql/mysql_tushare/etc/mysql/conf.d:/etc/mysql/conf.d   -e MYSQL_ROOT_PASSWORD=tushare_root   -e MYSQL_DATABASE=tushare   -e MYSQL_USER=tushare   -e MYSQL_PASSWORD=tushare mysql
```

##### 创建用户

```sql
CREATE USER 'stock'@'%' IDENTIFIED BY 'stock';
CREATE DATABASE stock;
GRANT ALL PRIVILEGES ON stock.* TO 'stock'@'%';
ALTER USER 'stock'@'%' IDENTIFIED WITH mysql_native_password BY 'stock';
FLUSH PRIVILEGES;
```


####  修改配置文件信息(本地数据库地址信息)

编辑 application.ini 修改本地数据库的地址用户密码
```
[mysql]
host=127.0.0.1
user=stock
password=stock
port=3310
database=stock
```

##  执行同步

```shell
python data_sun.py
```

## MySQL 结果数据示列

日线行情前复权（stock_daily_qfq）
说明：前复权历史数据可能为负值, 通常模型预测使用前复权数据，收益累加计算使用后复权数据
```
trade_date|symbol|open |close|high |low  |vol      |amount       |avg               |amp  |pct_chg|pct_amt|tr  |
----------+------+-----+-----+-----+-----+---------+-------------+------------------+-----+-------+-------+----+
2024-04-26|000001|10.59| 10.6|10.67|10.48|1607628.0|1698581436.96|10.565761712037858| 1.79|  -0.09|  -0.01|0.83|
2024-04-25|000001| 10.5|10.61|10.62|10.48|1113812.0|1176304524.86|10.561068877512541| 1.33|   0.76|   0.08|0.57|
2024-04-24|000001|10.52|10.53|10.57|10.46| 941568.0| 989093538.21|10.504748867952182| 1.04|  -0.09|  -0.01|0.49|
2024-04-23|000001|10.51|10.54|10.65|10.46|1240027.0|1308866093.13|  10.5551418890879| 1.81|   0.38|   0.04|0.64|
2024-04-22|000001|10.64| 10.5|10.81|10.45|2009818.0|2125722741.41|10.576692722475368| 3.37|  -1.78|  -0.19|1.04|
2024-04-19|000001|10.71|10.69|10.82|10.66|1457675.0|1562376146.58|10.718274969248975| 1.48|  -1.02|  -0.11|0.75|
2024-04-18|000001|10.58| 10.8|11.03|10.56|3165914.0|3427338981.81| 10.82574884159835| 4.43|   1.69|   0.18|1.63|
2024-04-17|000001|10.26|10.62|10.63|10.21|2232641.0|2337576586.88| 10.47000653880315| 4.09|   3.31|   0.34|1.15|
2024-04-16|000001|10.28|10.28|10.39|10.22|1478036.0|1523138499.07|10.305151559704905| 1.65|   -0.1|  -0.01|0.76|
2024-04-15|000001|10.07|10.29|10.32|10.06|1453203.0|1486326196.01| 10.22793233987268| 2.58|   2.29|   0.23|0.75|
2024-04-12|000001|10.22|10.06|10.27|10.04|1305454.0|1322011753.93|10.126835215411651| 2.24|  -1.85|  -0.19|0.67|
2024-04-11|000001|10.24|10.25|10.29|10.12|1010213.0|1030820137.39|10.203988044006561| 1.65|  -0.29|  -0.03|0.52|
2024-04-10|000001|10.38|10.28|10.41|10.27|1246391.0|1288696196.29|10.339421548214002| 1.35|  -1.15|  -0.12|0.64|
```

## 同步表清单

| MySQL表名                                                   | Akshare 接口名                                                                                 | 数据说明                                                          |  
|:----------------------------------------------------------|:--------------------------------------------------------------------------------------------|:--------------------------------------------------------------|  
| [stock_basic_info](stock_basic_info/stock_basic_info.sql) | stock_info_sh_name_code, stock_info_sz_name_code, stock_info_bj_name_code, stock_hk_spot_em | [股票基本信息](stock_basic_info/stock_basic_info.py)(每日全量覆盖)        |  
| [stock_daily_qfq](stock_daily_qfq/stock_daily_qfq.sql)    | stock_zh_a_hist, stock_hk_hist                                                              | [股票日行情信息-前复权](stock_daily_qfq/stock_daily_qfq.py) (每日增量同步)    |  
| [stock_min30_qfq](stock_min30_qfq/stock_min30_qfq.sql)    | stock_zh_a_hist_min_em, stock_hk_hist_min_em                                                | [股票30分钟行情信息-前复权](stock_min30_qfq/stock_min30_qfq.py) (每日增量同步) |  


## 执行日志说明

相关日志打印存储在 项目根目录/logs/data_syn.log 文件中, 日志示例

```

```

## 其他

欢迎提问或 提交 Bug / PR   


