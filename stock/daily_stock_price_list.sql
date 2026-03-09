-- 首先，如果表存在则删除（谨慎操作，仅用于初始化环境）
DROP TABLE IF EXISTS `daily_stock_price_list`;

-- 创建优化后的日行情表
CREATE TABLE `daily_stock_price_list` (
  `secucode` varchar(10) NOT NULL COMMENT '证券代码',
  `date` date NOT NULL COMMENT '交易日期',
  `close_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价(元)',
  `volume` decimal(18,2) DEFAULT NULL COMMENT '成交量(手)',
  `pe_ratio` decimal(18,4) DEFAULT NULL COMMENT '市盈率(PE, TTM)',
  `pb_ratio` decimal(18,4) DEFAULT NULL COMMENT '市净率(PB, MRQ)',
  -- 设置复合主键，确保同一只股票在同一天只有一条记录
  PRIMARY KEY (`secucode`, `date`) USING BTREE,
  -- 建立外键约束，关联至股票基本信息表(stock_list)，确保代码有效性
  CONSTRAINT `fk_daily_price_stock`
    FOREIGN KEY (`secucode`)
    REFERENCES `stock_list` (`secucode`)
    ON DELETE RESTRICT   -- 若股票列表中的代码被行情表引用，则禁止删除
    ON UPDATE CASCADE,   -- 若股票列表中的代码更新，则同步更新此表
  -- 添加交易日期索引，便于按时间范围快速查询
  INDEX `idx_date` (`date`) USING BTREE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  ROW_FORMAT=DYNAMIC
  COMMENT='股票日行情表';