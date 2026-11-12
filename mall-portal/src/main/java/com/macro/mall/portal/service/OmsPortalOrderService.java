package com.macro.mall.portal.service;

import com.macro.mall.common.api.CommonPage;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.ConfirmOrderResult;
import com.macro.mall.portal.domain.OmsOrderDetail;
import com.macro.mall.portal.domain.OrderParam;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PayResult;

import java.util.List;
import java.util.Map;

/**
 * 前台订单管理 Service。
 *
 * <p>A 阶段事务边界约定：</p>
 * <ul>
 *   <li>事务注解统一放在<strong>实现类方法</strong>上并显式 {@code rollbackFor = Exception.class}，
 *       不再依赖接口上容易被误解的注解，也不依赖同类内部自调用触发代理。</li>
 *   <li>支付与取消的真实事务入口是独立的 {@link OrderStateTransitionService} Bean，
 *       保证 MQ 消费者 / 定时任务逐单调用时每笔订单都是独立事务。</li>
 *   <li>{@code cancelTimeOutOrder()} 本身<strong>不加事务</strong>，由它逐单调用状态机。</li>
 * </ul>
 */
public interface OmsPortalOrderService {

    /** 根据用户购物车信息生成确认单信息。 */
    ConfirmOrderResult generateConfirmOrder(List<Long> cartIds);

    /** 根据提交信息生成订单（锁库存与订单写入在同一事务）。 */
    Map<String, Object> generateOrder(OrderParam orderParam);

    /** 内部/隔离测试环境的支付成功入口，无第三方金额可核对。 */
    Integer paySuccess(Long orderId, Integer payType);

    /** 可信支付结果入口（支付宝回调 / 主动查询），会核对订单与金额。 */
    PayResult paySuccessByOrderSn(PayCommand command);

    /** 分页扫描超时订单并逐单取消，每个订单独立事务。 */
    Integer cancelTimeOutOrder();

    /** 用户主动取消：校验订单归属当前登录会员。 */
    void cancelUserOrder(Long orderId);

    /** 系统侧逐单取消（超时消息 / 定时扫描）：以数据库固定到期时间校验，返回可判定的结果。 */
    CancelResult cancelBySystem(Long orderId);

    /** B 阶段：为历史订单补齐本地消息记录（不再直接发 MQ）。 */
    void sendDelayMessageCancelOrder(Long orderId);

    /** 确认收货。 */
    void confirmReceiveOrder(Long orderId);

    /** 分页获取用户订单。 */
    CommonPage<OmsOrderDetail> list(Integer status, Integer pageNum, Integer pageSize);

    /** 根据订单 ID 获取订单详情。 */
    OmsOrderDetail detail(Long orderId);

    /** 用户根据订单 ID 删除订单。 */
    void deleteOrder(Long orderId);

    /** 根据 orderSn 获取订单（不过滤状态，供重复通知与金额核对使用）。 */
    OmsOrder getOrderByOrderSn(String orderSn);
}
