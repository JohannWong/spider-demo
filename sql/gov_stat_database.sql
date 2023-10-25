CREATE DATABASE `gov_data_stat`
/*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */
/*!80016 DEFAULT ENCRYPTION='N' */;

-- gov_data_stat.hgyd_menu definition
CREATE TABLE `gov_data_stat`.`hgyd_menu` (
  `id` varchar(20) COLLATE utf8mb4_0900_bin NOT NULL,
  `db_code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
  `is_parent` tinyint(1) NOT NULL,
  `name` varchar(200) COLLATE utf8mb4_0900_bin DEFAULT NULL,
  `pid` varchar(20) COLLATE utf8mb4_0900_bin DEFAULT NULL,
  `wd_code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT NULL,
  `active` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- gov_data_stat.hgyd_data definition
CREATE TABLE `gov_data_stat`.`hgyd_data` (
  `code` varchar(30) COLLATE utf8mb4_0900_bin NOT NULL,
  `data` decimal(10,4) NOT NULL,
  `str_data` varchar(10) COLLATE utf8mb4_0900_bin DEFAULT NULL,
  `dot_count` int NOT NULL,
  `has_data` tinyint(1) NOT NULL,
  `zb_code` varchar(20) COLLATE utf8mb4_0900_bin NOT NULL,
  `sj_code` varchar(10) COLLATE utf8mb4_0900_bin NOT NULL,
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
