-- =============================================================================
-- B 阶段：迁移前只读预检（订单到期时间 / 本地消息表）
--
-- 作用：在改动任何数据之前，统计
--   1. 当前数据库是否已完成 A 阶段 / 是否已有 B 阶段结构；
--   2. 历史待支付订单数量、是否缺少 create_time、是否缺少 expire_time；
--   3. 按约定的历史超时时长回填后，有多少订单会被判定为「已经到期」；
--   4. 这些订单是否已有本地消息记录；
--   5. 无法确定原超时时长的订单清单；
--   6. 超时兜底扫描 SQL 的执行计划是否命中索引。
--
-- 本脚本对「列/表尚不存在」的老库同样可用：相关查询按 information_schema
-- 的结果动态拼装。【不修改任何数据】。
--
-- 执行：mysql -h127.0.0.1 -P3307 -uroot -p mall < document/sql/mall_b_precheck.sql
-- =============================================================================

SET NAMES utf8mb4;

SET @has_expire_col = (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order' AND COLUMN_NAME = 'expire_time');
SET @has_outbox = (SELECT COUNT(*) FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order_message');
SET @has_anomaly = (SELECT COUNT(*) FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order_pay_anomaly');
SET @stock_not_null = (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'pms_sku_stock'
      AND COLUMN_NAME = 'stock' AND IS_NULLABLE = 'NO');
SET @has_scan_index = (SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order' AND INDEX_NAME = 'idx_order_expire_scan');

SELECT '== 1. A / B 阶段结构检查 ==' AS section;
SELECT @has_anomaly      AS a_migration_table_exists,
       @stock_not_null   AS stock_not_null,
       @has_expire_col   AS expire_time_column_exists,
       @has_outbox       AS outbox_table_exists,
       @has_scan_index   AS expire_scan_index_exists;

-- 约定：历史订单按「create_time + 约定的历史超时时长（分钟）」回填。
-- 若历史配置与当前不同，请在 mall_b_migration.sql 中显式修改 @legacy_overtime。
SET @legacy_overtime = (SELECT COALESCE(MAX(normal_order_overtime), 120) FROM oms_order_setting WHERE id = 1);

SET @missing_expire_expr = IF(@has_expire_col = 1, 'SUM(expire_time IS NULL)', "'N/A (列不存在)'");
SET @sql = CONCAT(
    'SELECT ''== 2. 历史待支付订单总览 =='' AS section, ',
    'COUNT(*) AS pending_orders, ',
    'SUM(create_time IS NULL) AS missing_create_time, ',
    @missing_expire_expr, ' AS missing_expire_time, ',
    'MIN(create_time) AS oldest_create_time, MAX(create_time) AS newest_create_time ',
    'FROM oms_order WHERE status = 0 AND delete_status = 0');
PREPARE s2 FROM @sql; EXECUTE s2; DEALLOCATE PREPARE s2;

SELECT '== 3. 回填规则与影响范围（务必先备份并人工确认） ==' AS section;
SELECT @legacy_overtime AS legacy_overtime_minutes;

SET @expire_filter = IF(@has_expire_col = 1, 'o.expire_time IS NULL', '1 = 1');
SET @msg_expr = IF(@has_outbox = 1,
    '(SELECT COUNT(*) FROM oms_order_message m WHERE m.order_id = o.id AND m.event_type = ''ORDER_TIMEOUT_CANCEL'')',
    '0');
SET @sql = CONCAT(
    'SELECT o.id AS order_id, o.order_sn, o.create_time, ',
    'DATE_ADD(o.create_time, INTERVAL ', @legacy_overtime, ' MINUTE) AS would_be_expire_time, ',
    'DATE_ADD(o.create_time, INTERVAL ', @legacy_overtime, ' MINUTE) <= NOW() AS already_expired, ',
    @msg_expr, ' AS existing_messages ',
    'FROM oms_order o WHERE o.status = 0 AND o.delete_status = 0 AND ', @expire_filter,
    ' ORDER BY o.id');
PREPARE s3 FROM @sql; EXECUTE s3; DEALLOCATE PREPARE s3;

SELECT '== 4. 无法确定原超时时长的订单（必须列入待处理，不能随意假设） ==' AS section;
SELECT o.id AS order_id, o.order_sn, o.create_time, 'CREATE_TIME_MISSING' AS reason
FROM oms_order o
WHERE o.status = 0 AND o.delete_status = 0 AND o.create_time IS NULL
ORDER BY o.id;

SELECT '== 5. 超时兜底扫描执行计划（期望命中 idx_order_expire_scan） ==' AS section;
SET @sql = IF(@has_expire_col = 1,
    CONCAT('EXPLAIN SELECT o.id FROM oms_order o WHERE o.status = 0 AND o.delete_status = 0 ',
           'AND o.expire_time IS NOT NULL AND o.expire_time <= NOW() AND o.id > 0 ORDER BY o.id LIMIT 100'),
    'SELECT ''expire_time 列尚不存在，跳过 EXPLAIN（迁移后再执行本脚本确认索引）'' AS note');
PREPARE s5 FROM @sql; EXECUTE s5; DEALLOCATE PREPARE s5;

SELECT '== 6. 预检完成，未修改数据 ==' AS section;
