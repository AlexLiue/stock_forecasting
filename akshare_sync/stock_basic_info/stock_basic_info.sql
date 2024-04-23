-- stock.stock_info_a_code_name definition


USE `stock`;
DROP TABLE IF EXISTS `stock_basic_info`;
CREATE TABLE `stock_basic_info`
(
    `symbol`         varchar(12)          DEFAULT NULL COMMENT '证券代码',
    `name`              varchar(64)         DEFAULT NULL COMMENT '证券简称',
    `exchange`           varchar(4)         DEFAULT NULL COMMENT '交易所',
    `market`             varchar(4)         DEFAULT NULL COMMENT '板块:主板/创业板/科创板/CDR',
    `list_date`         varchar(12)         DEFAULT NULL COMMENT '上市日期',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
     KEY `stock_basic_symbol` (`symbol`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci COMMENT ='股票基本信息';



