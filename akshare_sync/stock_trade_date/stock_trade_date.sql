-- stock.stock_info_a_code_name definition


USE `stock`;
DROP TABLE IF EXISTS `stock_trade_date`;
CREATE TABLE `stock_trade_date`
(

    `exchange`           varchar(4)         DEFAULT NULL COMMENT '交易所',
    `trade_date`         date               DEFAULT NULL COMMENT '交易日期',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
     UNIQUE KEY `stock_trade_date_exchange` (`exchange`,`trade_date`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci COMMENT ='股票交易日历';



