-- =============================================================================
-- B 阶段：已有数据库迁移脚本（订单到期时间 + 本地消息表）
--
-- ！！！执行前必读 ！！！
-- 1. 必须先完成 A 阶段迁移（mall_a_migration.sql），B 依赖订单条件状态流转。
-- 2. 先执行 document/sql/mall_b_precheck.sql，确认历史待支付订单的影响范围。
-- 3. 备份 oms_order / oms_order_item / oms_order_message / pms_sku_stock。
-- 4. 第 2 节的 @legacy_overtime 是【约定的历史超时时长（分钟）】。
--    若历史上改过超时配置，必须按实际值修改，不能默认沿用当前配置。
-- 5. 缺少 create_time 的历史订单无法推断原到期时间，会写入
--    oms_order_expire_backfill_exception 等待人工处理，不会被随意假设。
--
-- 新环境不需要本脚本，直接使用 document/sql/mall.sql（已内置同样结构）。
-- 执行：mysql -h127.0.0.1 -P3307 -uroot -p mall < document/sql/mall_b_migration.sql
-- =============================================================================

-- 脚本内含中文失败原因，显式声明客户端字符集，避免依赖 mysql 客户端默认编码。
SET NAMES utf8mb4;

-- -----------------------------------------------------------------------------
-- 第 0 节：守卫。A 阶段结构未就绪时拒绝继续。
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `mall_b_assert_phase_a_ready`;
DELIMITER $$
CREATE PROCEDURE `mall_b_assert_phase_a_ready`()
BEGIN
    IF (SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order_pay_anomaly') = 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'A 阶段结构缺失：请先执行 mall_a_migration.sql，B 依赖订单条件状态更新';
    END IF;
    IF (SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'pms_sku_stock'
          AND COLUMN_NAME = 'stock' AND IS_NULLABLE = 'YES') > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'A 阶段结构缺失：pms_sku_stock.stock 仍允许 NULL，请先执行 mall_a_migration.sql';
    END IF;
END$$
DELIMITER ;
CALL `mall_b_assert_phase_a_ready`();
DROP PROCEDURE IF EXISTS `mall_b_assert_phase_a_ready`;

-- -----------------------------------------------------------------------------
-- 第 1 节：oms_order 增加固定到期时间列与扫描索引（幂等）。
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `mall_b_add_expire_column`;
DELIMITER $$
CREATE PROCEDURE `mall_b_add_expire_column`()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order' AND COLUMN_NAME = 'expire_time'
    ) THEN
        ALTER TABLE `oms_order`
            ADD COLUMN `expire_time` datetime NULL DEFAULT NULL COMMENT '待支付订单固定过期时间' AFTER `modify_time`;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order' AND INDEX_NAME = 'idx_order_expire_scan'
    ) THEN
        -- (status, delete_status, id, expire_time)：keyset 分页按 id 顺序走覆盖索引，
        -- 避免 (status, delete_status, expire_time, id) 在 ORDER BY id 时产生 filesort。
        ALTER TABLE `oms_order`
            ADD INDEX `idx_order_expire_scan` (`status`, `delete_status`, `id`, `expire_time`);
    END IF;
END$$
DELIMITER ;
CALL `mall_b_add_expire_column`();
DROP PROCEDURE IF EXISTS `mall_b_add_expire_column`;

-- -----------------------------------------------------------------------------
-- 第 2 节：本地消息表与失败记录表（结构与 document/sql/mall.sql 保持一致）。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `oms_order_message`  (
  `id`               bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '主键',
  `event_id`         varchar(64) NOT NULL COMMENT '全局唯一事件ID，所有重试保持不变',
  `order_id`         bigint(20)  NOT NULL COMMENT '订单ID',
  `event_type`       varchar(64) NOT NULL COMMENT '事件类型：ORDER_TIMEOUT_CANCEL',
  `payload`          json        NOT NULL COMMENT '消息体（只放标识，不放订单/库存明细）',
  `due_time`         datetime    NOT NULL COMMENT '到期时间，与 oms_order.expire_time 一致',
  `status`           varchar(16) NOT NULL COMMENT 'PENDING / SENDING / SENT / FAILED',
  `retry_count`      int(11)     NOT NULL DEFAULT 0 COMMENT '连续发送失败次数',
  `next_retry_time`  datetime    NOT NULL COMMENT '下次可领取时间',
  `lease_owner`      varchar(64) NULL DEFAULT NULL COMMENT '当前领取实例标识',
  `lease_until`      datetime    NULL DEFAULT NULL COMMENT '租约截止时间，过期后可被重新领取',
  `attempt_version`  bigint(20)  NOT NULL DEFAULT 0 COMMENT '领取版本号，防止迟到的旧回调覆盖新尝试',
  `last_error`       varchar(1000) NULL DEFAULT NULL COMMENT '最近一次失败原因',
  `created_at`       datetime    NOT NULL COMMENT '创建时间',
  `updated_at`       datetime    NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uk_event_id` (`event_id`),
  UNIQUE KEY `uk_order_event` (`order_id`, `event_type`),
  KEY `idx_dispatch` (`status`, `next_retry_time`, `due_time`, `id`),
  KEY `idx_lease` (`status`, `lease_until`),
  KEY `idx_created_at` (`created_at`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '订单超时取消本地消息表';

CREATE TABLE IF NOT EXISTS `oms_order_consume_failure`  (
  `id`            bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '主键',
  `event_id`      varchar(64) NULL DEFAULT NULL COMMENT '事件ID（无法解析时为空）',
  `order_id`      bigint(20)  NULL DEFAULT NULL COMMENT '订单ID（无法解析时为空）',
  `raw_payload`   text        NULL COMMENT '原始消息体',
  `reason`        varchar(1000) NOT NULL COMMENT '失败原因',
  `retry_count`   int(11)     NOT NULL DEFAULT 0 COMMENT '消费端已重试次数',
  `handle_status` int(1)      NOT NULL DEFAULT 0 COMMENT '0 待处理；1 已人工处理',
  `created_at`    datetime    NOT NULL COMMENT '记录时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_event_id` (`event_id`),
  KEY `idx_handle_status` (`handle_status`, `created_at`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '订单取消消息消费失败记录';

CREATE TABLE IF NOT EXISTS `oms_order_expire_backfill_exception`  (
  `order_id`      bigint(20)  NOT NULL COMMENT '订单ID',
  `order_sn`      varchar(64) NULL DEFAULT NULL COMMENT '订单号',
  `reason`        varchar(255) NOT NULL COMMENT '无法回填的原因',
  `handle_status` int(1)      NOT NULL DEFAULT 0 COMMENT '0 待处理；1 已处理',
  `created_at`    datetime    NOT NULL COMMENT '记录时间',
  PRIMARY KEY (`order_id`) USING BTREE,
  KEY `idx_handle_status` (`handle_status`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '历史订单到期时间回填待处理记录';

-- -----------------------------------------------------------------------------
-- 第 3 节：历史待支付订单回填。
-- 规则：expire_time = create_time + 约定的历史超时时长。
--       缺少 create_time 的订单不猜测，登记为待人工处理。
-- -----------------------------------------------------------------------------
-- ↓↓↓ 按业务实际历史配置修改；默认取当前 oms_order_setting 的值，仅作为起点 ↓↓↓
SET @legacy_overtime = (SELECT COALESCE(MAX(normal_order_overtime), 120) FROM oms_order_setting WHERE id = 1);
SELECT '== 本次回填采用的历史超时时长（分钟）==' AS section, @legacy_overtime AS legacy_overtime_minutes;

INSERT INTO `oms_order_expire_backfill_exception`
    (`order_id`, `order_sn`, `reason`, `handle_status`, `created_at`)
SELECT o.id, o.order_sn, '历史待支付订单缺少 create_time，无法确定原超时时长', 0, NOW()
FROM oms_order o
WHERE o.status = 0 AND o.delete_status = 0 AND o.create_time IS NULL
  AND NOT EXISTS (SELECT 1 FROM oms_order_expire_backfill_exception e WHERE e.order_id = o.id);

UPDATE oms_order
SET expire_time = DATE_ADD(create_time, INTERVAL @legacy_overtime MINUTE)
WHERE status = 0 AND delete_status = 0
  AND expire_time IS NULL AND create_time IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 第 4 节：为历史待支付订单补齐 PENDING 本地消息。
-- 事件 ID 固定为 OTC-<orderId>，与 order_id + event_type 唯一约束一致；
-- 已存在记录（包括 FAILED）用 NOT EXISTS 跳过并保留，等待人工重试。
-- 这里刻意不使用 INSERT IGNORE：它会把字段异常也一并吞掉。
-- -----------------------------------------------------------------------------
INSERT INTO `oms_order_message`
    (`event_id`, `order_id`, `event_type`, `payload`, `due_time`, `status`,
     `retry_count`, `next_retry_time`, `lease_owner`, `lease_until`,
     `attempt_version`, `last_error`, `created_at`, `updated_at`)
SELECT
    CONCAT('OTC-', o.id),
    o.id,
    'ORDER_TIMEOUT_CANCEL',
    JSON_OBJECT('schemaVersion', 1,
                'eventId', CONCAT('OTC-', o.id),
                'eventType', 'ORDER_TIMEOUT_CANCEL',
                'orderId', o.id,
                'dueTime', DATE_FORMAT(o.expire_time, '%Y-%m-%d %H:%i:%s')),
    o.expire_time,
    'PENDING',
    0,
    GREATEST(NOW(), o.expire_time),
    NULL, NULL, 0, NULL, NOW(), NOW()
FROM oms_order o
WHERE o.status = 0 AND o.delete_status = 0 AND o.expire_time IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM oms_order_message m
                  WHERE m.order_id = o.id AND m.event_type = 'ORDER_TIMEOUT_CANCEL');

-- -----------------------------------------------------------------------------
-- 第 5 节：验证。
-- -----------------------------------------------------------------------------
SELECT '== 迁移后校验 ==' AS section;
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'oms_order' AND COLUMN_NAME = 'expire_time';
SHOW INDEX FROM `oms_order` WHERE Key_name = 'idx_order_expire_scan';
SELECT status, COUNT(*) AS messages FROM oms_order_message GROUP BY status;
SELECT COUNT(*) AS backfill_exceptions FROM oms_order_expire_backfill_exception WHERE handle_status = 0;
SELECT COUNT(*) AS pending_without_expire_time
FROM oms_order WHERE status = 0 AND delete_status = 0 AND expire_time IS NULL;
