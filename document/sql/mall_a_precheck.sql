-- =============================================================================
-- A 阶段：pms_sku_stock 历史数据预检（只读，不修改任何数据）
-- 适用：已经在运行的数据库。新环境请直接使用更新后的 document/sql/mall.sql。
-- 执行方式：mysql -h127.0.0.1 -P3307 -uroot -p mall < document/sql/mall_a_precheck.sql
--
-- 规则：stock = 实际库存，lock_stock = 待支付订单占用库存，可售 = stock - lock_stock。
-- 条件更新 SQL 拒绝在 NULL / 负数 / lock_stock > stock 的记录上执行，
-- 因此必须先人工确认异常数据来源，禁止直接把异常值批量改成 0。
-- =============================================================================

SELECT '== 1. 总览：异常记录计数 ==' AS section;
SELECT
    COUNT(*)                                                              AS total_rows,
    SUM(stock IS NULL)                                                    AS stock_null,
    SUM(lock_stock IS NULL)                                               AS lock_stock_null,
    SUM(stock < 0)                                                        AS stock_negative,
    SUM(lock_stock < 0)                                                   AS lock_stock_negative,
    SUM(lock_stock > stock)                                               AS lock_stock_gt_stock,
    SUM(stock IS NULL OR lock_stock IS NULL OR stock < 0
        OR lock_stock < 0 OR lock_stock > stock)                          AS anomaly_rows
FROM pms_sku_stock;

SELECT '== 2. 明细：需要人工确认的 SKU ==' AS section;
SELECT
    id, product_id, sku_code, stock, lock_stock,
    CASE
        WHEN stock IS NULL OR lock_stock IS NULL THEN 'NULL 字段'
        WHEN stock < 0 OR lock_stock < 0         THEN '负数'
        WHEN lock_stock > stock                  THEN '锁定库存大于实际库存'
    END AS anomaly_type
FROM pms_sku_stock
WHERE stock IS NULL OR lock_stock IS NULL OR stock < 0
   OR lock_stock < 0 OR lock_stock > stock
ORDER BY id;

SELECT '== 3. 交叉核对：待付款订单明细与其 SKU 的锁定库存 ==' AS section;
-- 每条异常 SKU 上仍有多少「待付款」订单占用量；用于判断 lock_stock 是否被
-- 历史订单真实占用，必要时按订单逐笔重算，不能整体清零。
SELECT
    s.id                        AS sku_id,
    s.sku_code,
    s.stock,
    s.lock_stock,
    SUM(CASE WHEN o.id IS NOT NULL THEN COALESCE(oi.product_quantity, 0) ELSE 0 END)
                                AS pending_order_quantity,
    COUNT(DISTINCT o.id)        AS pending_order_count
FROM pms_sku_stock s
LEFT JOIN oms_order_item oi ON oi.product_sku_id = s.id
LEFT JOIN oms_order o       ON o.id = oi.order_id
                           AND o.status = 0
                           AND o.delete_status = 0
WHERE s.stock IS NULL OR s.lock_stock IS NULL OR s.stock < 0
   OR s.lock_stock < 0 OR s.lock_stock > s.stock
GROUP BY s.id, s.sku_code, s.stock, s.lock_stock
ORDER BY s.id;

SELECT '== 4. 待付款订单明细引用的 SKU 是否存在 ==' AS section;
SELECT oi.product_sku_id, COUNT(*) AS item_count, SUM(oi.product_quantity) AS quantity
FROM oms_order_item oi
JOIN oms_order o ON o.id = oi.order_id AND o.status = 0 AND o.delete_status = 0
LEFT JOIN pms_sku_stock s ON s.id = oi.product_sku_id
WHERE s.id IS NULL OR oi.product_sku_id IS NULL OR oi.product_quantity IS NULL
   OR oi.product_quantity <= 0
GROUP BY oi.product_sku_id;

SELECT '== 5. 列定义现状（确认是否仍允许 NULL） ==' AS section;
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'pms_sku_stock'
  AND COLUMN_NAME IN ('stock', 'lock_stock');

-- 输出为空 = 数据可以直接进入迁移脚本；
-- 输出非空 = 先逐条确认来源（例如历史订单锁定量、演示数据、人工改库），再决定修复值。
