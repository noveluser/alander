/*
 Navicat Premium Data Transfer

 Source Server         : mysql_test
 Source Server Type    : MySQL
 Source Server Version : 80024
 Source Host           : 10.31.9.24:3306
 Source Schema         : ics

 Target Server Type    : MySQL
 Target Server Version : 80024
 File Encoding         : 65001

 Date: 04/08/2025 15:56:08
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for temp_bags
-- ----------------------------
DROP TABLE IF EXISTS `temp_bags`;
CREATE TABLE `temp_bags`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `bsm_time` timestamp(6) NULL DEFAULT NULL,
  `latest_time` timestamp(6) NULL DEFAULT NULL,
  `lpc` bigint NULL DEFAULT NULL,
  `pid` bigint ,
  `current_location` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `orginal_destination` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `final_destination` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `flightnr` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `STD` timestamp NULL DEFAULT NULL,
  `tubid` int NULL DEFAULT NULL,
  `scaned` tinyint(1) NULL DEFAULT NULL,
  `checked` tinyint(1) NULL DEFAULT NULL,
  `registed` tinyint(1) NULL DEFAULT NULL,
  `sorted` tinyint(1) NULL DEFAULT NULL,
  `status` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `flightnrtype` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  INDEX `id_index`(`id`) USING BTREE,
  INDEX `lpc_index`(`lpc`) USING BTREE,
  INDEX `pid_index`(`pid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;

SET FOREIGN_KEY_CHECKS = 1;
