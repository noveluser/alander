DROP TABLE IF EXISTS `sort_finance`;
CREATE TABLE `sort_finance`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `report_time` date NOT NULL COMMENT '报告期（财报日期，如 2023-12-31 代表年报）',
  `secucode` varchar(10) NOT NULL COMMENT '证券代码',
  `name` varchar(100) NOT NULL COMMENT '公司名称',
  `fixed_asset` decimal(20,4) NULL DEFAULT NULL COMMENT '固定资产',
  `total_assets` decimal(20,4) NULL DEFAULT NULL COMMENT '总资产',
  `total_liabilities` decimal(20,4) NULL DEFAULT NULL COMMENT '总负债',
  `share_capital` decimal(20,4) NULL DEFAULT NULL COMMENT '股本',
  `total_equity` decimal(20,4) NULL DEFAULT NULL COMMENT '净资产（所有者权益合计）',
  `operate_income` decimal(20,4) NULL DEFAULT NULL COMMENT '营业收入',
  `estimate_total_revenue` decimal(20,4) NULL DEFAULT NULL COMMENT '预估总营业收入',
  `deduct_parent_netprofit` decimal(20,4) NULL DEFAULT NULL COMMENT '扣非归母净利润',
  `estimate_total_netprofit` decimal(20,4) NULL DEFAULT NULL COMMENT '预估净利润',
  `netcash_operate` decimal(20,4) NULL DEFAULT NULL COMMENT '经营活动现金流量净额',
  `construct_long_asset` decimal(20,4) NULL DEFAULT NULL COMMENT '构建长期资产支付的现金',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_secucode_report_time`(`secucode`, `report_time`) USING BTREE COMMENT '证券代码+报告期唯一索引，防止重复数据',
  INDEX `idx_report_time`(`report_time`) USING BTREE COMMENT '报告期索引，便于按时间查询',
  INDEX `idx_secucode`(`secucode`) USING BTREE COMMENT '证券代码索引'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = DYNAMIC COMMENT = '财务数据表';

SET FOREIGN_KEY_CHECKS = 1;