-- stock.daily definition

-- 日线
DROP TABLE IF EXISTS `daily_n_average`;
CREATE TABLE `daily_n_average`
(
    `ts_code`      varchar(16)        DEFAULT NULL COMMENT '股票代码',
    `trade_date`   int                DEFAULT NULL COMMENT '交易日期',

    `avg_1`     double             DEFAULT NULL COMMENT '1日线涨跌额',
    `avg_2`     double             DEFAULT NULL COMMENT '2日线涨跌额',
    `avg_3`     double             DEFAULT NULL COMMENT '3日线涨跌额',
    `avg_4`     double             DEFAULT NULL COMMENT '4日线涨跌额',
    `avg_5`     double             DEFAULT NULL COMMENT '5日线涨跌额',
    `avg_6`     double             DEFAULT NULL COMMENT '6日线涨跌额',
    `avg_7`     double             DEFAULT NULL COMMENT '7日线涨跌额',
    `avg_8`     double             DEFAULT NULL COMMENT '8日线涨跌额',
    `avg_9`     double             DEFAULT NULL COMMENT '9日线涨跌额',
    `avg_10`    double             DEFAULT NULL COMMENT '10日线涨跌额',
    `avg_11`    double             DEFAULT NULL COMMENT '11日线涨跌额',
    `avg_12`    double             DEFAULT NULL COMMENT '12日线涨跌额',
    `avg_13`    double             DEFAULT NULL COMMENT '13日线涨跌额',
    `avg_14`    double             DEFAULT NULL COMMENT '14日线涨跌额',
    `avg_15`    double             DEFAULT NULL COMMENT '15日线涨跌额',
    `avg_16`    double             DEFAULT NULL COMMENT '16日线涨跌额',
    `avg_18`    double             DEFAULT NULL COMMENT '18日线涨跌额',
    `avg_21`    double             DEFAULT NULL COMMENT '21日线涨跌额',
    `avg_31`    double             DEFAULT NULL COMMENT '31日线涨跌额',
    `avg_45`    double             DEFAULT NULL COMMENT '45日线涨跌额',
    `avg_61`    double             DEFAULT NULL COMMENT '61日线涨跌额',
    `avg_91`    double             DEFAULT NULL COMMENT '61日线涨跌额',
    `avg_123`    double             DEFAULT NULL COMMENT '123日线涨跌额',
    `avg_187`    double             DEFAULT NULL COMMENT '187日线涨跌额',
    `avg_365`    double             DEFAULT NULL COMMENT '365日线涨跌额',
    `avg_731`    double             DEFAULT NULL COMMENT '731日线涨跌额',
    `avg_1095`   double             DEFAULT NULL COMMENT '1095日线涨跌额',
    `avg_99999`  double             DEFAULT NULL COMMENT '99999日线涨跌额',

    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    UNIQUE KEY `daily_ts_code_idx` (`ts_code`, `trade_date`) USING BTREE,
    UNIQUE KEY `daily_trade_date_idx` (`trade_date`, `ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci COMMENT ='A股N日线行情'
    /*!50100 PARTITION BY RANGE (`trade_date`)
    (PARTITION p1990 VALUES LESS THAN (19901231) ENGINE = InnoDB,
    PARTITION p1991 VALUES LESS THAN (19911231) ENGINE = InnoDB,
    PARTITION p1992 VALUES LESS THAN (19921231) ENGINE = InnoDB,
    PARTITION p1993 VALUES LESS THAN (19931231) ENGINE = InnoDB,
    PARTITION p1994 VALUES LESS THAN (19941231) ENGINE = InnoDB,
    PARTITION p1995 VALUES LESS THAN (19951231) ENGINE = InnoDB,
    PARTITION p1996 VALUES LESS THAN (19961231) ENGINE = InnoDB,
    PARTITION p1997 VALUES LESS THAN (19971231) ENGINE = InnoDB,
    PARTITION p1998 VALUES LESS THAN (19981231) ENGINE = InnoDB,
    PARTITION p1999 VALUES LESS THAN (19991231) ENGINE = InnoDB,
    PARTITION p2000 VALUES LESS THAN (20001231) ENGINE = InnoDB,
    PARTITION p2001 VALUES LESS THAN (20011231) ENGINE = InnoDB,
    PARTITION p2002 VALUES LESS THAN (20021231) ENGINE = InnoDB,
    PARTITION p2003 VALUES LESS THAN (20031231) ENGINE = InnoDB,
    PARTITION p2004 VALUES LESS THAN (20041231) ENGINE = InnoDB,
    PARTITION p2005 VALUES LESS THAN (20051231) ENGINE = InnoDB,
    PARTITION p2006 VALUES LESS THAN (20061231) ENGINE = InnoDB,
    PARTITION p2007 VALUES LESS THAN (20071231) ENGINE = InnoDB,
    PARTITION p2008 VALUES LESS THAN (20081231) ENGINE = InnoDB,
    PARTITION p2009 VALUES LESS THAN (20091231) ENGINE = InnoDB,
    PARTITION p2010 VALUES LESS THAN (20101231) ENGINE = InnoDB,
    PARTITION p2011 VALUES LESS THAN (20111231) ENGINE = InnoDB,
    PARTITION p2012 VALUES LESS THAN (20121231) ENGINE = InnoDB,
    PARTITION p2013 VALUES LESS THAN (20131231) ENGINE = InnoDB,
    PARTITION p2014 VALUES LESS THAN (20141231) ENGINE = InnoDB,
    PARTITION p2015 VALUES LESS THAN (20151231) ENGINE = InnoDB,
    PARTITION p2016 VALUES LESS THAN (20161231) ENGINE = InnoDB,
    PARTITION p2017 VALUES LESS THAN (20171231) ENGINE = InnoDB,
    PARTITION p2018 VALUES LESS THAN (20181231) ENGINE = InnoDB,
    PARTITION p2019 VALUES LESS THAN (20191231) ENGINE = InnoDB,
    PARTITION p2020 VALUES LESS THAN (20201231) ENGINE = InnoDB,
    PARTITION p2021 VALUES LESS THAN (20211231) ENGINE = InnoDB,
    PARTITION p2022 VALUES LESS THAN (20221231) ENGINE = InnoDB,
    PARTITION p2023 VALUES LESS THAN (20231231) ENGINE = InnoDB,
    PARTITION p2024 VALUES LESS THAN (20241231) ENGINE = InnoDB,
    PARTITION p2025 VALUES LESS THAN (20251231) ENGINE = InnoDB,
    PARTITION p2026 VALUES LESS THAN (20261231) ENGINE = InnoDB,
    PARTITION p2027 VALUES LESS THAN (20271231) ENGINE = InnoDB,
    PARTITION p2028 VALUES LESS THAN (20281231) ENGINE = InnoDB,
    PARTITION p2029 VALUES LESS THAN (20291231) ENGINE = InnoDB,
    PARTITION p2030 VALUES LESS THAN (20301231) ENGINE = InnoDB,
    PARTITION p2031 VALUES LESS THAN (20311231) ENGINE = InnoDB,
    PARTITION p2032 VALUES LESS THAN (20321231) ENGINE = InnoDB,
    PARTITION p2033 VALUES LESS THAN (20331231) ENGINE = InnoDB,
    PARTITION p2034 VALUES LESS THAN (20341231) ENGINE = InnoDB,
    PARTITION p2035 VALUES LESS THAN (20351231) ENGINE = InnoDB,
    PARTITION p2036 VALUES LESS THAN (20361231) ENGINE = InnoDB,
    PARTITION p2037 VALUES LESS THAN (20371231) ENGINE = InnoDB,
    PARTITION p2038 VALUES LESS THAN (20381231) ENGINE = InnoDB,
    PARTITION p2039 VALUES LESS THAN (20391231) ENGINE = InnoDB,
    PARTITION p2040 VALUES LESS THAN (20401231) ENGINE = InnoDB,
    PARTITION p2041 VALUES LESS THAN (20411231) ENGINE = InnoDB,
    PARTITION p2042 VALUES LESS THAN (20421231) ENGINE = InnoDB,
    PARTITION p2043 VALUES LESS THAN (20431231) ENGINE = InnoDB,
    PARTITION p2044 VALUES LESS THAN (20441231) ENGINE = InnoDB,
    PARTITION p2045 VALUES LESS THAN (20451231) ENGINE = InnoDB) */;
