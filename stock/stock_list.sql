DROP TABLE IF EXISTS `stock_list`;
CREATE TABLE `stock_list` (
  `secucode` varchar(10) NOT NULL COMMENT '证券代码',
  `name` varchar(100) NOT NULL COMMENT '公司名称',
  PRIMARY KEY (`secucode`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = DYNAMIC COMMENT = '股票名称';

SET FOREIGN_KEY_CHECKS = 1;