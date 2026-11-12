-- =============================================================================
-- A 阶段：已有数据库迁移脚本（订单状态与库存一致性改造）
--
-- ！！！执行前必读 ！！！
-- 1. 先执行 document/sql/mall_a_precheck.sql，逐条确认异常库存来源。
-- 2. 本脚本的「第 0 节守卫」会在仍有异常库存时直接中止，防止把历史锁定量
--    静默清零。守卫通过后才继续。
-- 3. 请在业务低峰期执行，并先备份 pms_sku_stock / oms_order / oms_order_item。
-- 4. 本脚本只做 DDL 与新增表，不修改任何业务数据。
--
-- 新环境不需要本脚本，直接使用 document/sql/mall.sql（已内置同样结构）。
-- 执行：mysql -h127.0.0.1 -P3307 -uroot -p mall < document/sql/mall_a_migration.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 第 0 节：守卫。存在 NULL / 负数 / lock_stock > stock 时拒绝继续。
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `mall_a_assert_stock_clean`;
DELIMITER $$
CREATE PROCEDURE `mall_a_assert_stock_clean`()
BEGIN
    DECLARE v_bad INT DEFAULT 0;
    SELECT COUNT(*) INTO v_bad
    FROM pms_sku_stock
    WHERE stock IS NULL OR lock_stock IS NULL
       OR stock < 0 OR lock_stock < 0 OR lock_stock > stock;
    IF v_bad > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'pms_sku_stock 仍有异常数据，请先执行 mall_a_precheck.sql 并人工修复，禁止直接迁移';
    END IF;
END$$
DELIMITER ;
CALL `mall_a_assert_stock_clean`();
DROP PROCEDURE IF EXISTS `mall_a_assert_stock_clean`;

-- 如果守卫失败，下面是「人工确认后」才可以使用的修复模板，默认全部注释。
-- 注意：只有确认这些 lock_stock 不对应任何待付款订单时才可清零；
--       否则应按 oms_order_item 逐笔重算锁定量。
-- UPDATE pms_sku_stock SET stock = 0      WHERE stock IS NULL OR stock < 0;
-- UPDATE pms_sku_stock SET lock_stock = 0 WHERE lock_stock IS NULL OR lock_stock < 0;

-- -----------------------------------------------------------------------------
-- 第 1 节：支付成功但本地订单状态不匹配时，留下可追踪的待对账/退款记录。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `oms_order_pay_anomaly` (
  `id`             bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
  `order_id`       bigint(20)   NOT NULL COMMENT '本地订单ID',
  `order_sn`       varchar(64)  NOT NULL DEFAULT '' COMMENT '商户订单号',
  `member_id`      bigint(20)   NULL DEFAULT NULL COMMENT '会员ID',
  `order_status`   int(1)       NOT NULL COMMENT '收到支付结果时本地订单状态',
  `pay_amount`     decimal(10,2) NULL DEFAULT NULL COMMENT '本地应付金额',
  `notify_amount`  decimal(10,2) NULL DEFAULT NULL COMMENT '第三方通知金额',
  `trade_no`       varchar(64)  NOT NULL DEFAULT '' COMMENT '第三方交易号',
  `pay_type`       int(1)       NULL DEFAULT NULL COMMENT '支付方式',
  `reason`         varchar(255) NOT NULL COMMENT '异常原因',
  `raw_params`     text         NULL COMMENT '原始回调参数，便于对账',
  `handle_status`  int(1)       NOT NULL DEFAULT 0 COMMENT '0 待处理；1 已退款；2 已人工处理',
  `create_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
  `modify_time`    datetime     NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uk_order_trade` (`order_id`, `trade_no`),
  KEY `idx_handle_status` (`handle_status`, `create_time`),
  KEY `idx_order_sn` (`order_sn`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '支付成功但订单状态不匹配，待对账或退款';

-- -----------------------------------------------------------------------------
-- 第 2 节：超时扫描索引。分页查询条件为 status=0 / delete_status=0 / create_time。
-- MySQL 8.0 不支持 ADD INDEX IF NOT EXISTS，用 information_schema 判断后动态执行。
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `mall_a_add_index_if_missing`;
DELIMITER $$
CREATE PROCEDURE `mall_a_add_index_if_missing`()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_order'
          AND INDEX_NAME = 'idx_order_timeout_scan'
    ) THEN
        ALTER TABLE `oms_order`
            ADD INDEX `idx_order_timeout_scan` (`status`, `delete_status`, `create_time`);
    END IF;
END$$
DELIMITER ;
CALL `mall_a_add_index_if_missing`();
DROP PROCEDURE IF EXISTS `mall_a_add_index_if_missing`;

-- -----------------------------------------------------------------------------
-- 第 3 节：收紧库存列定义。条件更新 SQL 已经显式拒绝 NULL，
-- 这里把数据库列也改成 NOT NULL DEFAULT 0，形成第二道防线。
-- -----------------------------------------------------------------------------
ALTER TABLE `pms_sku_stock`
    MODIFY COLUMN `stock`      int(11) NOT NULL DEFAULT 0 COMMENT '库存',
    MODIFY COLUMN `lock_stock` int(11) NOT NULL DEFAULT 0 COMMENT '锁定库存';

-- -----------------------------------------------------------------------------
-- 第 4 节（可选，需评估后台改价/改库存流程后再启用）：数据库层 CHECK 约束。
-- 启用后任何写入都无法制造负数或超锁定；但后台若把 stock 改到低于现有
-- lock_stock，会直接报错而不是静默产生不一致，请先评估业务影响。
-- -----------------------------------------------------------------------------
-- ALTER TABLE `pms_sku_stock`
--     ADD CONSTRAINT `chk_sku_stock_non_negative` CHECK (`stock` >= 0),
--     ADD CONSTRAINT `chk_sku_lock_non_negative`  CHECK (`lock_stock` >= 0),
--     ADD CONSTRAINT `chk_sku_lock_le_stock`      CHECK (`lock_stock` <= `stock`);

-- -----------------------------------------------------------------------------
-- 第 5 节：验证
-- -----------------------------------------------------------------------------
SELECT '== 迁移后校验 ==' AS section;
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'pms_sku_stock'
  AND COLUMN_NAME IN ('stock', 'lock_stock');
SHOW INDEX FROM `oms_order` WHERE Key_name = 'idx_order_timeout_scan';
SHOW TABLES LIKE 'oms_order_pay_anomaly';
