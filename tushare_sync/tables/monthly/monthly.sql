-- stock.monthly definition

DROP TABLE IF EXISTS `monthly`;
CREATE TABLE `monthly`
(
    `ts_code`      varchar(16)        DEFAULT NULL COMMENT '股票代码',
    `trade_date`   int                DEFAULT NULL COMMENT '交易日期',
    `close`        double             DEFAULT NULL COMMENT '月收盘价',
    `open`         double             DEFAULT NULL COMMENT '月开盘价',
    `high`         double             DEFAULT NULL COMMENT '月最高价',
    `low`          double             DEFAULT NULL COMMENT '月最低价',
    `pre_close`    double             DEFAULT NULL COMMENT '上月收盘价',
    `change`       double             DEFAULT NULL COMMENT '月涨跌额',
    `pct_chg`      double             DEFAULT NULL COMMENT '月涨跌幅 （未复权，如果是复权请用 通用行情接口 ）',
    `vol`          double             DEFAULT NULL COMMENT '月成交量',
    `amount`       double             DEFAULT NULL COMMENT '月成交额',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    UNIQUE KEY `daily_ts_code_idx` (`ts_code`, `trade_date`) USING BTREE,
    UNIQUE KEY `daily_trade_date_idx` (`trade_date`, `ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci COMMENT ='沪深股票-行情数据-A股月线行情'
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
