package com.macro.mall.portal.dao;

import com.macro.mall.portal.domain.OmsOrderPayAnomaly;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 前台订单管理自定义 Dao。
 *
 * <p>A 阶段（订单状态与库存一致性改造）后，本 Dao 的所有写操作均为
 * <strong>单条带条件的原子 UPDATE</strong>，返回影响行数由服务层判定：
 * 1 = 成功，0 = SKU 不存在 / 数据异常 / 可售不足 / 状态已被其他请求改变。
 * 不再提供「先查再按主键覆盖写」或批量 CASE WHEN 更新的入口。</p>
 *
 * <p>多 SKU 由服务层按 skuId 升序逐个调用，每次都在同一数据库事务内，
 * 任一 SKU 失败即抛异常回滚全部变更。</p>
 */
public interface PortalOrderDao {

    /** 下单：原子锁定库存。影响行数必须为 1。 */
    int lockSkuStock(@Param("skuId") Long skuId, @Param("quantity") Integer quantity);

    /** 支付成功：原子扣减实际库存并释放锁定库存。影响行数必须为 1。 */
    int deductSkuStock(@Param("skuId") Long skuId, @Param("quantity") Integer quantity);

    /** 取消：原子释放锁定库存。影响行数必须为 1。 */
    int releaseSkuStock(@Param("skuId") Long skuId, @Param("quantity") Integer quantity);

    /** 支付：订单状态 0 → 1 的条件更新，是支付与取消竞争的裁判。 */
    int markOrderPaid(@Param("orderId") Long orderId, @Param("payType") Integer payType);

    /** 取消：订单状态 0 → 4 的条件更新。 */
    int closePendingOrder(@Param("orderId") Long orderId);

    /**
     * 当前读（FOR UPDATE）查询订单状态。
     * 条件更新返回 0 行时用它分类处理，避免 REPEATABLE-READ 下普通快照读
     * 仍看到事务开始时的旧状态而误判。
     */
    Integer selectOrderStatusForUpdate(@Param("orderId") Long orderId);

    /**
     * 分页（keyset）查询超时未支付订单 ID。
     * 用 id &gt; afterId 推进游标，避免某个订单反复失败时死循环重复扫描同一批。
     */
    List<Long> listTimeOutOrderIds(@Param("minute") Integer minute,
                                   @Param("afterId") Long afterId,
                                   @Param("limit") Integer limit);

    /** 原子扣减积分：积分不足或字段为 NULL 时返回 0 行。 */
    int deductMemberIntegration(@Param("memberId") Long memberId, @Param("amount") Integer amount);

    /** 原子增加积分（取消时返还）。 */
    int addMemberIntegration(@Param("memberId") Long memberId, @Param("amount") Integer amount);

    /** 条件更新优惠券使用记录 0 → 1，一次只占用一张。 */
    int markCouponUsed(@Param("couponId") Long couponId, @Param("memberId") Long memberId);

    /** 条件更新优惠券使用记录 1 → 0，取消时恢复。 */
    int releaseCoupon(@Param("couponId") Long couponId, @Param("memberId") Long memberId);

    /** 记录「第三方支付成功但本地订单状态不匹配」的待对账异常，重复通知不重复插入。 */
    int insertPayAnomaly(OmsOrderPayAnomaly record);

    /** 启动时历史库存数据预检：返回 NULL / 负数 / 超锁定 的 SKU。 */
    List<Map<String, Object>> listStockAnomalies();

    // -------------------------------------------------------------------------
    // B 阶段：固定到期时间与超时兜底扫描
    // -------------------------------------------------------------------------

    /**
     * 下单事务内写入固定到期时间。MBG 生成的 OmsOrder 不含该列，
     * 因此用自定义 DAO 在 insert 之后追加一次条件 UPDATE，仍在同一事务里。
     */
    int updateOrderExpireTime(@Param("orderId") Long orderId, @Param("expireTime") Date expireTime);

    /** 系统取消时读取数据库中的固定到期时间；NULL 表示历史数据尚未回填。 */
    Date selectOrderExpireTime(@Param("orderId") Long orderId);

    /**
     * 超时兜底扫描：keyset 分页取「已到期但仍待支付」的订单 ID。
     * 命中 idx_order_expire_scan (status, delete_status, id, expire_time)。
     */
    List<Long> listExpiredPendingOrderIds(@Param("afterId") Long afterId,
                                          @Param("limit") Integer limit);
}
