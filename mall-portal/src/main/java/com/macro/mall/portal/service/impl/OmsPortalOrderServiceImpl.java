package com.macro.mall.portal.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.github.pagehelper.PageHelper;
import com.macro.mall.common.api.CommonPage;
import com.macro.mall.common.exception.Asserts;
import com.macro.mall.common.service.RedisService;
import com.macro.mall.mapper.OmsOrderItemMapper;
import com.macro.mall.mapper.OmsOrderMapper;
import com.macro.mall.mapper.OmsOrderSettingMapper;
import com.macro.mall.mapper.PmsSkuStockMapper;
import com.macro.mall.mapper.UmsIntegrationConsumeSettingMapper;
import com.macro.mall.model.*;
import com.macro.mall.portal.component.CancelOrderSender;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.dao.PortalOrderItemDao;
import com.macro.mall.portal.domain.*;
import com.macro.mall.portal.exception.OrderConsistencyException;
import com.macro.mall.portal.service.OmsCartItemService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.macro.mall.portal.service.OrderStateTransitionService;
import com.macro.mall.portal.service.UmsMemberCacheService;
import com.macro.mall.portal.service.UmsMemberCouponService;
import com.macro.mall.portal.service.UmsMemberReceiveAddressService;
import com.macro.mall.portal.service.UmsMemberService;
import com.macro.mall.portal.util.OrderSkuAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 前台订单管理 Service。
 *
 * <p>A 阶段（订单状态与库存一致性改造）：</p>
 * <ul>
 *   <li>锁库存改为 MySQL 条件更新，按 SKU 汇总、按 skuId 升序逐条执行，检查影响行数。</li>
 *   <li>库存锁定、订单写入、券/积分、购物车清理在同一个数据库事务内，任一失败一起回滚。</li>
 *   <li>修复 useIntegration 在 insert 之后设置导致未持久化、取消时无法返还的问题。</li>
 *   <li>延迟取消消息改到事务提交后发送；MQ 失败不阻断下单，由定时扫描兜底。</li>
 *   <li>支付与取消委托给独立 Bean {@link OrderStateTransitionService}，避免自调用事务失效。</li>
 * </ul>
 */
@Slf4j
@Service
public class OmsPortalOrderServiceImpl implements OmsPortalOrderService {

    /** 单次超时扫描的分页大小；keyset 分页用 id 游标推进。 */
    private static final int TIME_OUT_SCAN_BATCH_SIZE = 100;
    /** 单次扫描最多处理的页数，防止异常数据导致长循环。 */
    private static final int TIME_OUT_SCAN_MAX_BATCHES = 20;

    @Autowired
    private UmsMemberService memberService;
    @Autowired
    private OmsCartItemService cartItemService;
    @Autowired
    private UmsMemberReceiveAddressService memberReceiveAddressService;
    @Autowired
    private UmsMemberCouponService memberCouponService;
    @Autowired
    private UmsIntegrationConsumeSettingMapper integrationConsumeSettingMapper;
    @Autowired
    private PmsSkuStockMapper skuStockMapper;
    @Autowired
    private OmsOrderMapper orderMapper;
    @Autowired
    private PortalOrderItemDao orderItemDao;
    @Autowired
    private RedisService redisService;
    @Value("${redis.key.orderId}")
    private String REDIS_KEY_ORDER_ID;
    @Value("${redis.database}")
    private String REDIS_DATABASE;
    @Autowired
    private PortalOrderDao portalOrderDao;
    @Autowired
    private OmsOrderSettingMapper orderSettingMapper;
    @Autowired
    private OmsOrderItemMapper orderItemMapper;
    @Autowired
    private CancelOrderSender cancelOrderSender;
    @Autowired
    private OrderStateTransitionService orderStateTransitionService;
    @Autowired
    private UmsMemberCacheService memberCacheService;

    @Override
    public ConfirmOrderResult generateConfirmOrder(List<Long> cartIds) {
        ConfirmOrderResult result = new ConfirmOrderResult();
        UmsMember currentMember = memberService.getCurrentMember();
        List<CartPromotionItem> cartPromotionItemList = cartItemService.listPromotion(currentMember.getId(), cartIds);
        result.setCartPromotionItemList(cartPromotionItemList);
        result.setMemberReceiveAddressList(memberReceiveAddressService.list());
        List<SmsCouponHistoryDetail> couponHistoryDetailList = memberCouponService.listCart(cartPromotionItemList, 1);
        result.setCouponHistoryDetailList(couponHistoryDetailList);
        result.setMemberIntegration(currentMember.getIntegration());
        UmsIntegrationConsumeSetting integrationConsumeSetting = integrationConsumeSettingMapper.selectByPrimaryKey(1L);
        result.setIntegrationConsumeSetting(integrationConsumeSetting);
        result.setCalcAmount(calcCartAmount(cartPromotionItemList));
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> generateOrder(OrderParam orderParam) {
        // ---------- 1. 输入校验 ----------
        if (orderParam == null || orderParam.getMemberReceiveAddressId() == null) {
            Asserts.fail("请选择收货地址！");
        }
        UmsMember currentMember = memberService.getCurrentMember();
        if (currentMember == null || currentMember.getId() == null) {
            Asserts.fail("请先登录！");
        }
        if (currentMember.getIntegration() == null) {
            currentMember.setIntegration(0);
        }
        List<CartPromotionItem> cartPromotionItemList =
                cartItemService.listPromotion(currentMember.getId(), orderParam.getCartIds());
        if (CollectionUtils.isEmpty(cartPromotionItemList)) {
            Asserts.fail("购物车中没有可下单的商品！");
        }
        for (CartPromotionItem item : cartPromotionItemList) {
            if (item.getProductSkuId() == null || item.getQuantity() == null
                    || item.getQuantity() <= 0 || item.getPrice() == null) {
                Asserts.fail("购物车商品信息不完整或购买数量非法！");
            }
        }
        // getItem 内部已按当前会员过滤，null 表示地址不存在或不属于当前用户
        UmsMemberReceiveAddress address = memberReceiveAddressService.getItem(orderParam.getMemberReceiveAddressId());
        if (address == null) {
            Asserts.fail("收货地址不存在或不属于当前用户！");
        }

        // ---------- 2. 按 SKU 汇总、升序、原子锁库存 ----------
        // hasStock() 的提前检查不能消除「检查与更新之间」的竞争窗口，
        // 因此是否成功只以条件更新的影响行数为准。
        List<SkuQuantity> skuQuantities = OrderSkuAggregator.fromCartItems(cartPromotionItemList);
        lockStockAtomically(skuQuantities);

        // ---------- 3. 生成订单明细 ----------
        List<OmsOrderItem> orderItemList = new ArrayList<>();
        for (CartPromotionItem cartPromotionItem : cartPromotionItemList) {
            OmsOrderItem orderItem = new OmsOrderItem();
            orderItem.setProductId(cartPromotionItem.getProductId());
            orderItem.setProductName(cartPromotionItem.getProductName());
            orderItem.setProductPic(cartPromotionItem.getProductPic());
            orderItem.setProductAttr(cartPromotionItem.getProductAttr());
            orderItem.setProductBrand(cartPromotionItem.getProductBrand());
            orderItem.setProductSn(cartPromotionItem.getProductSn());
            orderItem.setProductPrice(cartPromotionItem.getPrice());
            orderItem.setProductQuantity(cartPromotionItem.getQuantity());
            orderItem.setProductSkuId(cartPromotionItem.getProductSkuId());
            orderItem.setProductSkuCode(cartPromotionItem.getProductSkuCode());
            orderItem.setProductCategoryId(cartPromotionItem.getProductCategoryId());
            orderItem.setPromotionAmount(cartPromotionItem.getReduceAmount());
            orderItem.setPromotionName(cartPromotionItem.getPromotionMessage());
            orderItem.setGiftIntegration(cartPromotionItem.getIntegration());
            orderItem.setGiftGrowth(cartPromotionItem.getGrowth());
            orderItemList.add(orderItem);
        }

        // ---------- 4. 优惠券与积分金额 ----------
        boolean hasCoupon = orderParam.getCouponId() != null;
        if (!hasCoupon) {
            for (OmsOrderItem orderItem : orderItemList) {
                orderItem.setCouponAmount(BigDecimal.ZERO);
            }
        } else {
            SmsCouponHistoryDetail couponHistoryDetail = getUseCoupon(cartPromotionItemList, orderParam.getCouponId());
            if (couponHistoryDetail == null) {
                Asserts.fail("该优惠券不可用");
            }
            handleCouponAmount(orderItemList, couponHistoryDetail);
        }
        Integer rawUseIntegration = orderParam.getUseIntegration();
        int usedIntegration = rawUseIntegration == null ? 0 : Math.max(rawUseIntegration, 0);
        if (usedIntegration == 0) {
            for (OmsOrderItem orderItem : orderItemList) {
                orderItem.setIntegrationAmount(BigDecimal.ZERO);
            }
        } else {
            BigDecimal totalAmount = calcTotalAmount(orderItemList);
            BigDecimal integrationAmount =
                    getUseIntegrationAmount(usedIntegration, totalAmount, currentMember, hasCoupon);
            if (integrationAmount.compareTo(BigDecimal.ZERO) == 0) {
                Asserts.fail("积分不可用");
            }
            for (OmsOrderItem orderItem : orderItemList) {
                BigDecimal perAmount = orderItem.getProductPrice()
                        .divide(totalAmount, 3, RoundingMode.HALF_EVEN)
                        .multiply(integrationAmount);
                orderItem.setIntegrationAmount(perAmount);
            }
        }
        handleRealAmount(orderItemList);

        // ---------- 5. 组装订单 ----------
        OmsOrder order = new OmsOrder();
        order.setDiscountAmount(BigDecimal.ZERO);
        order.setTotalAmount(calcTotalAmount(orderItemList));
        order.setFreightAmount(BigDecimal.ZERO);
        order.setPromotionAmount(calcPromotionAmount(orderItemList));
        order.setPromotionInfo(getOrderPromotionInfo(orderItemList));
        if (hasCoupon) {
            order.setCouponId(orderParam.getCouponId());
            order.setCouponAmount(calcCouponAmount(orderItemList));
        } else {
            order.setCouponAmount(BigDecimal.ZERO);
        }
        order.setIntegrationAmount(calcIntegrationAmount(orderItemList));
        // 修复：原代码在 insert 之后才 setUseIntegration，导致该字段从未持久化，
        // 取消订单时无法返还已使用的积分。
        order.setUseIntegration(usedIntegration);
        order.setPayAmount(calcPayAmount(order));

        order.setMemberId(currentMember.getId());
        order.setCreateTime(new Date());
        order.setMemberUsername(currentMember.getUsername());
        int payType = orderParam.getPayType() == null ? 0 : orderParam.getPayType();
        order.setPayType(payType);
        order.setSourceType(1);
        order.setStatus(0);
        order.setOrderType(0);
        order.setReceiverName(address.getName());
        order.setReceiverPhone(address.getPhoneNumber());
        order.setReceiverPostCode(address.getPostCode());
        order.setReceiverProvince(address.getProvince());
        order.setReceiverCity(address.getCity());
        order.setReceiverRegion(address.getRegion());
        order.setReceiverDetailAddress(address.getDetailAddress());
        order.setConfirmStatus(0);
        order.setDeleteStatus(0);
        order.setIntegration(calcGifIntegration(orderItemList));
        order.setGrowth(calcGiftGrowth(orderItemList));
        order.setOrderSn(generateOrderSn(order));
        List<OmsOrderSetting> orderSettings = orderSettingMapper.selectByExample(new OmsOrderSettingExample());
        if (CollUtil.isNotEmpty(orderSettings)) {
            order.setAutoConfirmDay(orderSettings.get(0).getConfirmOvertime());
        }

        // ---------- 6. 写订单与明细（与锁库存同一事务） ----------
        orderMapper.insert(order);
        for (OmsOrderItem orderItem : orderItemList) {
            orderItem.setOrderId(order.getId());
            orderItem.setOrderSn(order.getOrderSn());
        }
        orderItemDao.insertList(orderItemList);

        // ---------- 7. 优惠券与积分：条件更新 / 原子增减 ----------
        if (hasCoupon) {
            int affected = portalOrderDao.markCouponUsed(orderParam.getCouponId(), currentMember.getId());
            if (affected != 1) {
                throw new OrderConsistencyException(
                        "优惠券使用状态更新失败（couponId=" + orderParam.getCouponId() + "），下单已回滚");
            }
        }
        if (usedIntegration > 0) {
            int affected = portalOrderDao.deductMemberIntegration(currentMember.getId(), usedIntegration);
            if (affected != 1) {
                throw new OrderConsistencyException("积分不足或积分数据异常，下单已回滚");
            }
            evictMemberCache(currentMember.getId());
        }

        // ---------- 8. 清理购物车 ----------
        deleteCartItemList(cartPromotionItemList, currentMember);

        // ---------- 9. 事务提交后发送延迟取消消息 ----------
        // 数据库事务与 MQ 发布不是同一个原子操作：放在事务内会出现
        // 「订单回滚但消息已发」和「订单提交但消息丢失」。这里改为 afterCommit 发送，
        // 发送失败只告警，由定时超时扫描兜底（完整可靠投递属于 B）。
        registerCancelMessageAfterCommit(order.getId());

        Map<String, Object> result = new HashMap<>();
        result.put("order", order);
        result.put("orderItemList", orderItemList);
        return result;
    }

    @Override
    public Integer paySuccess(Long orderId, Integer payType) {
        PayResult result = orderStateTransitionService.paySuccess(PayCommand.internal(orderId, payType));
        if (result.getStatus() == PayResult.Status.REJECTED) {
            Asserts.fail(result.getMessage());
        }
        if (result.getStatus() == PayResult.Status.LATE_PAYMENT_RECORDED) {
            log.error("支付晚到，已记录待对账异常：orderId={}", result.getOrderId());
        }
        return result.getStockRows();
    }

    @Override
    public PayResult paySuccessByOrderSn(PayCommand command) {
        // 事务在独立的 OrderStateTransitionService Bean 上，不再用 this.paySuccess() 自调用。
        return orderStateTransitionService.paySuccess(command);
    }

    /**
     * 超时扫描兜底：分页取候选 ID，逐单通过独立 Bean 执行条件取消。
     * 本方法【不加事务】，每单在状态机里拥有独立事务，异常订单不会拖垮整批。
     */
    @Override
    public Integer cancelTimeOutOrder() {
        OmsOrderSetting orderSetting = orderSettingMapper.selectByPrimaryKey(1L);
        if (orderSetting == null || orderSetting.getNormalOrderOvertime() == null
                || orderSetting.getNormalOrderOvertime() <= 0) {
            log.warn("订单超时时间配置缺失，跳过本次超时扫描");
            return 0;
        }
        int minute = orderSetting.getNormalOrderOvertime();
        long afterId = 0L;
        int handled = 0;
        int failed = 0;
        for (int batch = 0; batch < TIME_OUT_SCAN_MAX_BATCHES; batch++) {
            List<Long> ids = portalOrderDao.listTimeOutOrderIds(minute, afterId, TIME_OUT_SCAN_BATCH_SIZE);
            if (CollectionUtils.isEmpty(ids)) {
                break;
            }
            for (Long id : ids) {
                try {
                    CancelResult result = orderStateTransitionService.cancelOrder(
                            CancelCommand.bySystem(id, minute));
                    if (result.isSuccess()) {
                        handled++;
                    }
                } catch (Exception e) {
                    failed++;
                    log.error("逐单超时取消失败（本单事务已回滚，继续处理下一单）：orderId={}", id, e);
                }
                afterId = Math.max(afterId, id);
            }
            if (ids.size() < TIME_OUT_SCAN_BATCH_SIZE) {
                break;
            }
        }
        if (failed > 0) {
            // 失败告警：这些订单会保持在 status=0，下一轮扫描继续重试。
            log.error("超时扫描失败告警：本轮 {} 单取消失败，需排查库存或订单数据", failed);
        }
        return handled;
    }

    @Override
    public void cancelUserOrder(Long orderId) {
        UmsMember currentMember = memberService.getCurrentMember();
        if (currentMember == null || currentMember.getId() == null) {
            Asserts.fail("请先登录！");
        }
        CancelResult result = orderStateTransitionService.cancelOrder(
                CancelCommand.byUser(orderId, currentMember.getId()));
        switch (result.getStatus()) {
            case SUCCESS, ALREADY_CLOSED -> {
                // 重复取消视为幂等成功，不再次释放库存、券或积分
            }
            case REJECTED -> Asserts.fail(result.getMessage());
            default -> Asserts.fail("订单当前状态不允许取消");
        }
    }

    @Override
    public void cancelBySystem(Long orderId) {
        // 超时校验在状态机内完成：重复消息或提前投递不会错误释放库存。
        CancelResult result = orderStateTransitionService.cancelOrder(
                CancelCommand.bySystem(orderId, null));
        log.info("系统侧取消结果：orderId={}, status={}, message={}",
                orderId, result.getStatus(), result.getMessage());
    }

    @Override
    public void sendDelayMessageCancelOrder(Long orderId) {
        OmsOrderSetting orderSetting = orderSettingMapper.selectByPrimaryKey(1L);
        if (orderSetting == null || orderSetting.getNormalOrderOvertime() == null) {
            log.error("订单超时时间配置缺失，无法发送延迟取消消息：orderId={}", orderId);
            return;
        }
        long delayTimes = orderSetting.getNormalOrderOvertime() * 60L * 1000L;
        cancelOrderSender.sendMessage(orderId, delayTimes);
    }

    @Override
    public void confirmReceiveOrder(Long orderId) {
        UmsMember member = memberService.getCurrentMember();
        OmsOrder order = orderMapper.selectByPrimaryKey(orderId);
        if (!member.getId().equals(order.getMemberId())) {
            Asserts.fail("不能确认他人订单！");
        }
        if (order.getStatus() != 2) {
            Asserts.fail("该订单还未发货！");
        }
        order.setStatus(3);
        order.setConfirmStatus(1);
        order.setReceiveTime(new Date());
        orderMapper.updateByPrimaryKey(order);
    }

    @Override
    public CommonPage<OmsOrderDetail> list(Integer status, Integer pageNum, Integer pageSize) {
        if (status == -1) {
            status = null;
        }
        UmsMember member = memberService.getCurrentMember();
        PageHelper.startPage(pageNum, pageSize);
        OmsOrderExample orderExample = new OmsOrderExample();
        OmsOrderExample.Criteria criteria = orderExample.createCriteria();
        criteria.andDeleteStatusEqualTo(0).andMemberIdEqualTo(member.getId());
        if (status != null) {
            criteria.andStatusEqualTo(status);
        }
        orderExample.setOrderByClause("create_time desc");
        List<OmsOrder> orderList = orderMapper.selectByExample(orderExample);
        CommonPage<OmsOrder> orderPage = CommonPage.restPage(orderList);
        CommonPage<OmsOrderDetail> resultPage = new CommonPage<>();
        resultPage.setPageNum(orderPage.getPageNum());
        resultPage.setPageSize(orderPage.getPageSize());
        resultPage.setTotal(orderPage.getTotal());
        resultPage.setTotalPage(orderPage.getTotalPage());
        if (CollUtil.isEmpty(orderList)) {
            return resultPage;
        }
        List<Long> orderIds = orderList.stream().map(OmsOrder::getId).collect(Collectors.toList());
        OmsOrderItemExample orderItemExample = new OmsOrderItemExample();
        orderItemExample.createCriteria().andOrderIdIn(orderIds);
        List<OmsOrderItem> orderItemList = orderItemMapper.selectByExample(orderItemExample);
        List<OmsOrderDetail> orderDetailList = new ArrayList<>();
        for (OmsOrder omsOrder : orderList) {
            OmsOrderDetail orderDetail = new OmsOrderDetail();
            BeanUtil.copyProperties(omsOrder, orderDetail);
            List<OmsOrderItem> relatedItemList = orderItemList.stream()
                    .filter(item -> item.getOrderId().equals(orderDetail.getId()))
                    .collect(Collectors.toList());
            orderDetail.setOrderItemList(relatedItemList);
            orderDetailList.add(orderDetail);
        }
        resultPage.setList(orderDetailList);
        return resultPage;
    }

    @Override
    public OmsOrderDetail detail(Long orderId) {
        OmsOrder omsOrder = orderMapper.selectByPrimaryKey(orderId);
        OmsOrderItemExample example = new OmsOrderItemExample();
        example.createCriteria().andOrderIdEqualTo(orderId);
        List<OmsOrderItem> orderItemList = orderItemMapper.selectByExample(example);
        OmsOrderDetail orderDetail = new OmsOrderDetail();
        BeanUtil.copyProperties(omsOrder, orderDetail);
        orderDetail.setOrderItemList(orderItemList);
        return orderDetail;
    }

    @Override
    public void deleteOrder(Long orderId) {
        UmsMember member = memberService.getCurrentMember();
        OmsOrder order = orderMapper.selectByPrimaryKey(orderId);
        if (!member.getId().equals(order.getMemberId())) {
            Asserts.fail("不能删除他人订单！");
        }
        if (order.getStatus() == 3 || order.getStatus() == 4) {
            order.setDeleteStatus(1);
            orderMapper.updateByPrimaryKey(order);
        } else {
            Asserts.fail("只能删除已完成或已关闭的订单！");
        }
    }

    @Override
    public OmsOrder getOrderByOrderSn(String orderSn) {
        // 不过滤 status：重复支付通知、支付晚到都必须能查到订单并分类处理。
        // 发起支付时应由调用方额外校验 status == 0。
        OmsOrderExample example = new OmsOrderExample();
        example.createCriteria().andOrderSnEqualTo(orderSn).andDeleteStatusEqualTo(0);
        List<OmsOrder> orderList = orderMapper.selectByExample(example);
        return CollUtil.isNotEmpty(orderList) ? orderList.get(0) : null;
    }

    // ---------------------------------------------------------------------
    // 内部方法
    // ---------------------------------------------------------------------

    /** 按 SKU ID 升序逐个执行原子锁库存 SQL，任一失败抛异常让整个事务回滚。 */
    private void lockStockAtomically(List<SkuQuantity> skuQuantities) {
        for (SkuQuantity sku : skuQuantities) {
            int affected = portalOrderDao.lockSkuStock(sku.skuId(), sku.quantity());
            if (affected == 1) {
                continue;
            }
            // 区分「正常库存不足」与「历史脏数据」，便于排查且都不放行。
            PmsSkuStock stock = skuStockMapper.selectByPrimaryKey(sku.skuId());
            if (stock == null) {
                log.error("锁库存失败：SKU 不存在，skuId={}", sku.skuId());
                throw new OrderConsistencyException("SKU 不存在（skuId=" + sku.skuId() + "），下单已回滚");
            }
            if (stock.getStock() == null || stock.getLockStock() == null
                    || stock.getStock() < 0 || stock.getLockStock() < 0
                    || stock.getLockStock() > stock.getStock()) {
                log.error("锁库存失败：库存数据异常，skuId={}, stock={}, lockStock={}",
                        sku.skuId(), stock.getStock(), stock.getLockStock());
                throw new OrderConsistencyException(
                        "SKU 库存数据异常（skuId=" + sku.skuId() + "），请先执行历史数据预检");
            }
            log.info("锁库存失败：可售库存不足，skuId={}, stock={}, lockStock={}, quantity={}",
                    sku.skuId(), stock.getStock(), stock.getLockStock(), sku.quantity());
            Asserts.fail("库存不足，无法下单（SKU：" + sku.skuId() + "）");
        }
    }

    private void registerCancelMessageAfterCommit(Long orderId) {
        Runnable send = () -> {
            try {
                sendDelayMessageCancelOrder(orderId);
            } catch (Exception e) {
                // 消息丢失/发送失败不能让已提交的订单回滚；由定时扫描兜底并告警。
                log.error("订单已提交但延迟取消消息发送失败，依赖定时超时扫描兜底：orderId={}", orderId, e);
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send.run();
                }
            });
        } else {
            send.run();
        }
    }

    private void evictMemberCache(Long memberId) {
        try {
            memberCacheService.delMember(memberId);
        } catch (Exception e) {
            log.warn("积分已扣减但会员缓存失效失败：memberId={}", memberId, e);
        }
    }

    /** 生成 18 位订单编号：8 位日期 + 2 位平台 + 2 位支付方式 + 6 位以上自增 id。 */
    private String generateOrderSn(OmsOrder order) {
        StringBuilder sb = new StringBuilder();
        String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String key = REDIS_DATABASE + ":" + REDIS_KEY_ORDER_ID + date;
        Long increment = redisService.incr(key, 1);
        sb.append(date);
        sb.append(String.format("%02d", order.getSourceType()));
        sb.append(String.format("%02d", order.getPayType()));
        String incrementStr = increment.toString();
        if (incrementStr.length() <= 6) {
            sb.append(String.format("%06d", increment));
        } else {
            sb.append(incrementStr);
        }
        return sb.toString();
    }

    private void deleteCartItemList(List<CartPromotionItem> cartPromotionItemList, UmsMember currentMember) {
        List<Long> ids = new ArrayList<>();
        for (CartPromotionItem cartPromotionItem : cartPromotionItemList) {
            ids.add(cartPromotionItem.getId());
        }
        cartItemService.delete(currentMember.getId(), ids);
    }

    private Integer calcGiftGrowth(List<OmsOrderItem> orderItemList) {
        Integer sum = 0;
        for (OmsOrderItem orderItem : orderItemList) {
            sum = sum + orderItem.getGiftGrowth() * orderItem.getProductQuantity();
        }
        return sum;
    }

    private Integer calcGifIntegration(List<OmsOrderItem> orderItemList) {
        int sum = 0;
        for (OmsOrderItem orderItem : orderItemList) {
            sum += orderItem.getGiftIntegration() * orderItem.getProductQuantity();
        }
        return sum;
    }

    private void handleRealAmount(List<OmsOrderItem> orderItemList) {
        for (OmsOrderItem orderItem : orderItemList) {
            BigDecimal realAmount = orderItem.getProductPrice()
                    .subtract(orderItem.getPromotionAmount())
                    .subtract(orderItem.getCouponAmount())
                    .subtract(orderItem.getIntegrationAmount());
            orderItem.setRealAmount(realAmount);
        }
    }

    private String getOrderPromotionInfo(List<OmsOrderItem> orderItemList) {
        StringBuilder sb = new StringBuilder();
        for (OmsOrderItem orderItem : orderItemList) {
            sb.append(orderItem.getPromotionName());
            sb.append(";");
        }
        String result = sb.toString();
        if (result.endsWith(";")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private BigDecimal calcPayAmount(OmsOrder order) {
        return order.getTotalAmount()
                .add(order.getFreightAmount())
                .subtract(order.getPromotionAmount())
                .subtract(order.getCouponAmount())
                .subtract(order.getIntegrationAmount());
    }

    private BigDecimal calcIntegrationAmount(List<OmsOrderItem> orderItemList) {
        BigDecimal integrationAmount = BigDecimal.ZERO;
        for (OmsOrderItem orderItem : orderItemList) {
            if (orderItem.getIntegrationAmount() != null) {
                integrationAmount = integrationAmount.add(
                        orderItem.getIntegrationAmount().multiply(new BigDecimal(orderItem.getProductQuantity())));
            }
        }
        return integrationAmount;
    }

    private BigDecimal calcCouponAmount(List<OmsOrderItem> orderItemList) {
        BigDecimal couponAmount = BigDecimal.ZERO;
        for (OmsOrderItem orderItem : orderItemList) {
            if (orderItem.getCouponAmount() != null) {
                couponAmount = couponAmount.add(
                        orderItem.getCouponAmount().multiply(new BigDecimal(orderItem.getProductQuantity())));
            }
        }
        return couponAmount;
    }

    private BigDecimal calcPromotionAmount(List<OmsOrderItem> orderItemList) {
        BigDecimal promotionAmount = BigDecimal.ZERO;
        for (OmsOrderItem orderItem : orderItemList) {
            if (orderItem.getPromotionAmount() != null) {
                promotionAmount = promotionAmount.add(
                        orderItem.getPromotionAmount().multiply(new BigDecimal(orderItem.getProductQuantity())));
            }
        }
        return promotionAmount;
    }

    private BigDecimal getUseIntegrationAmount(Integer useIntegration, BigDecimal totalAmount,
                                               UmsMember currentMember, boolean hasCoupon) {
        BigDecimal zeroAmount = BigDecimal.ZERO;
        if (useIntegration.compareTo(currentMember.getIntegration()) > 0) {
            return zeroAmount;
        }
        UmsIntegrationConsumeSetting integrationConsumeSetting = integrationConsumeSettingMapper.selectByPrimaryKey(1L);
        if (hasCoupon && integrationConsumeSetting.getCouponStatus().equals(0)) {
            return zeroAmount;
        }
        if (useIntegration.compareTo(integrationConsumeSetting.getUseUnit()) < 0) {
            return zeroAmount;
        }
        BigDecimal integrationAmount = new BigDecimal(useIntegration)
                .divide(new BigDecimal(integrationConsumeSetting.getUseUnit()), 2, RoundingMode.HALF_EVEN);
        BigDecimal maxPercent = new BigDecimal(integrationConsumeSetting.getMaxPercentPerOrder())
                .divide(new BigDecimal(100), 2, RoundingMode.HALF_EVEN);
        if (integrationAmount.compareTo(totalAmount.multiply(maxPercent)) > 0) {
            return zeroAmount;
        }
        return integrationAmount;
    }

    private void handleCouponAmount(List<OmsOrderItem> orderItemList,
                                    SmsCouponHistoryDetail couponHistoryDetail) {
        SmsCoupon coupon = couponHistoryDetail.getCoupon();
        if (coupon.getUseType().equals(0)) {
            calcPerCouponAmount(orderItemList, coupon);
        } else if (coupon.getUseType().equals(1)) {
            calcPerCouponAmount(getCouponOrderItemByRelation(couponHistoryDetail, orderItemList, 0), coupon);
        } else if (coupon.getUseType().equals(2)) {
            calcPerCouponAmount(getCouponOrderItemByRelation(couponHistoryDetail, orderItemList, 1), coupon);
        }
    }

    private void calcPerCouponAmount(List<OmsOrderItem> orderItemList, SmsCoupon coupon) {
        BigDecimal totalAmount = calcTotalAmount(orderItemList);
        for (OmsOrderItem orderItem : orderItemList) {
            BigDecimal couponAmount = orderItem.getProductPrice()
                    .divide(totalAmount, 3, RoundingMode.HALF_EVEN)
                    .multiply(coupon.getAmount());
            orderItem.setCouponAmount(couponAmount);
        }
    }

    private List<OmsOrderItem> getCouponOrderItemByRelation(SmsCouponHistoryDetail couponHistoryDetail,
                                                            List<OmsOrderItem> orderItemList, int type) {
        List<OmsOrderItem> result = new ArrayList<>();
        if (type == 0) {
            List<Long> categoryIdList = new ArrayList<>();
            for (SmsCouponProductCategoryRelation relation : couponHistoryDetail.getCategoryRelationList()) {
                categoryIdList.add(relation.getProductCategoryId());
            }
            for (OmsOrderItem orderItem : orderItemList) {
                if (categoryIdList.contains(orderItem.getProductCategoryId())) {
                    result.add(orderItem);
                } else {
                    orderItem.setCouponAmount(BigDecimal.ZERO);
                }
            }
        } else if (type == 1) {
            List<Long> productIdList = new ArrayList<>();
            for (SmsCouponProductRelation relation : couponHistoryDetail.getProductRelationList()) {
                productIdList.add(relation.getProductId());
            }
            for (OmsOrderItem orderItem : orderItemList) {
                if (productIdList.contains(orderItem.getProductId())) {
                    result.add(orderItem);
                } else {
                    orderItem.setCouponAmount(BigDecimal.ZERO);
                }
            }
        }
        return result;
    }

    private SmsCouponHistoryDetail getUseCoupon(List<CartPromotionItem> cartPromotionItemList, Long couponId) {
        List<SmsCouponHistoryDetail> couponHistoryDetailList =
                memberCouponService.listCart(cartPromotionItemList, 1);
        for (SmsCouponHistoryDetail couponHistoryDetail : couponHistoryDetailList) {
            if (couponHistoryDetail.getCoupon().getId().equals(couponId)) {
                return couponHistoryDetail;
            }
        }
        return null;
    }

    private BigDecimal calcTotalAmount(List<OmsOrderItem> orderItemList) {
        BigDecimal totalAmount = BigDecimal.ZERO;
        for (OmsOrderItem item : orderItemList) {
            totalAmount = totalAmount.add(item.getProductPrice().multiply(new BigDecimal(item.getProductQuantity())));
        }
        return totalAmount;
    }

    private ConfirmOrderResult.CalcAmount calcCartAmount(List<CartPromotionItem> cartPromotionItemList) {
        ConfirmOrderResult.CalcAmount calcAmount = new ConfirmOrderResult.CalcAmount();
        calcAmount.setFreightAmount(BigDecimal.ZERO);
        BigDecimal totalAmount = BigDecimal.ZERO;
        BigDecimal promotionAmount = BigDecimal.ZERO;
        for (CartPromotionItem cartPromotionItem : cartPromotionItemList) {
            totalAmount = totalAmount.add(
                    cartPromotionItem.getPrice().multiply(new BigDecimal(cartPromotionItem.getQuantity())));
            promotionAmount = promotionAmount.add(
                    cartPromotionItem.getReduceAmount().multiply(new BigDecimal(cartPromotionItem.getQuantity())));
        }
        calcAmount.setTotalAmount(totalAmount);
        calcAmount.setPromotionAmount(promotionAmount);
        calcAmount.setPayAmount(totalAmount.subtract(promotionAmount));
        return calcAmount;
    }
}
