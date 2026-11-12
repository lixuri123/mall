package com.macro.mall.portal.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayClient;
import com.alipay.api.internal.util.AlipaySignature;
import com.alipay.api.request.AlipayTradePagePayRequest;
import com.alipay.api.request.AlipayTradeQueryRequest;
import com.alipay.api.request.AlipayTradeWapPayRequest;
import com.alipay.api.response.AlipayTradeQueryResponse;
import com.macro.mall.common.exception.Asserts;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.portal.config.AlipayConfig;
import com.macro.mall.portal.domain.AliPayParam;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PayResult;
import com.macro.mall.portal.service.AlipayService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 支付宝支付 Service。
 *
 * <p>A 阶段：验签只是第一步。回调还必须核对 app_id / 商户订单号 / 实付金额 /
 * 交易状态，再由订单状态机处理重复通知与「订单已取消但支付成功」。</p>
 */
@Slf4j
@Service
public class AlipayServiceImpl implements AlipayService {

    private static final String TRADE_SUCCESS = "TRADE_SUCCESS";
    private static final String TRADE_FINISHED = "TRADE_FINISHED";
    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_FAILURE = "failure";

    @Autowired
    private AlipayConfig alipayConfig;
    @Autowired
    private AlipayClient alipayClient;
    @Autowired
    private OmsPortalOrderService portalOrderService;

    @Override
    public String pay(AliPayParam aliPayParam) {
        AlipayTradePagePayRequest request = new AlipayTradePagePayRequest();
        fillCallbackUrls(request::setNotifyUrl, request::setReturnUrl);
        JSONObject bizContent = new JSONObject();
        bizContent.put("out_trade_no", aliPayParam.getOutTradeNo());
        BigDecimal totalAmount = requirePayableAmount(aliPayParam.getOutTradeNo());
        bizContent.put("total_amount", totalAmount);
        bizContent.put("subject", aliPayParam.getSubject());
        bizContent.put("product_code", "FAST_INSTANT_TRADE_PAY");
        request.setBizContent(bizContent.toString());
        try {
            return alipayClient.pageExecute(request).getBody();
        } catch (AlipayApiException e) {
            log.error("发起电脑网站支付异常", e);
            return null;
        }
    }

    @Override
    public String notify(Map<String, String> params) {
        boolean signVerified = false;
        try {
            signVerified = AlipaySignature.rsaCheckV1(params, alipayConfig.getAlipayPublicKey(),
                    alipayConfig.getCharset(), alipayConfig.getSignType());
        } catch (AlipayApiException e) {
            log.error("支付回调签名校验异常", e);
        }
        if (!signVerified) {
            log.warn("支付回调签名校验失败，拒绝处理");
            return RESULT_FAILURE;
        }

        // 1) 业务字段核对：应用 ID、订单号、实付金额、交易状态、第三方交易号。
        String appId = params.get("app_id");
        if (StrUtil.isNotEmpty(alipayConfig.getAppId()) && !alipayConfig.getAppId().equals(appId)) {
            log.warn("支付回调 app_id 不匹配，拒绝处理：appId={}", appId);
            return RESULT_FAILURE;
        }
        String tradeStatus = params.get("trade_status");
        if (!TRADE_SUCCESS.equals(tradeStatus) && !TRADE_FINISHED.equals(tradeStatus)) {
            // 已验签的非成功状态无需反复重投，记录后确认。
            log.info("支付回调交易未成功，跳过：trade_status={}", tradeStatus);
            return RESULT_SUCCESS;
        }
        String outTradeNo = params.get("out_trade_no");
        String tradeNo = params.get("trade_no");
        BigDecimal paidAmount = parseAmount(params.get("total_amount"));
        if (StrUtil.isBlank(outTradeNo) || StrUtil.isBlank(tradeNo) || paidAmount == null) {
            log.warn("支付回调缺少订单号/交易号/金额，拒绝处理：outTradeNo={}, tradeNo={}", outTradeNo, tradeNo);
            return RESULT_FAILURE;
        }

        // 2) 交给订单状态机：条件流转、扣库存、重复通知幂等、晚到支付落待对账记录。
        PayCommand command = PayCommand.fromGateway(outTradeNo, 1, paidAmount, tradeNo, params.toString());
        try {
            PayResult result = portalOrderService.paySuccessByOrderSn(command);
            switch (result.getStatus()) {
                case SUCCESS -> log.info("支付回调处理成功：orderSn={}, tradeNo={}", outTradeNo, tradeNo);
                case DUPLICATE -> log.info("重复支付通知，幂等返回：orderSn={}, tradeNo={}", outTradeNo, tradeNo);
                case LATE_PAYMENT_RECORDED -> log.error(
                        "支付晚到：订单已关闭/无效，已记录待对账异常，需退款或人工处理：orderSn={}, tradeNo={}",
                        outTradeNo, tradeNo);
                case REJECTED -> {
                    log.warn("支付回调被拒绝：orderSn={}, reason={}", outTradeNo, result.getMessage());
                    return RESULT_FAILURE;
                }
            }
            // LATE_PAYMENT_RECORDED 返回 success 不是「忽略」：异常已 REQUIRES_NEW 落库并 ERROR 告警，
            // 返回 success 只是避免第三方对同一笔已完成的支付无限重投。
            return RESULT_SUCCESS;
        } catch (Exception e) {
            log.error("支付回调处理异常，返回 failure 等待重投：orderSn={}, tradeNo={}", outTradeNo, tradeNo, e);
            return RESULT_FAILURE;
        }
    }

    @Override
    public String query(String outTradeNo, String tradeNo) {
        AlipayTradeQueryRequest request = new AlipayTradeQueryRequest();
        JSONObject bizContent = new JSONObject();
        if (StrUtil.isNotEmpty(outTradeNo)) {
            bizContent.put("out_trade_no", outTradeNo);
        }
        if (StrUtil.isNotEmpty(tradeNo)) {
            bizContent.put("trade_no", tradeNo);
        }
        String[] queryOptions = {"trade_settle_info"};
        bizContent.put("query_options", queryOptions);
        request.setBizContent(bizContent.toString());

        AlipayTradeQueryResponse response;
        try {
            response = alipayClient.execute(request);
        } catch (AlipayApiException e) {
            log.error("查询支付宝账单异常", e);
            return null;
        }
        if (response == null) {
            log.error("查询支付宝账单返回空响应");
            return null;
        }
        if (!response.isSuccess()) {
            log.error("查询支付宝账单失败：{}", response.getMsg());
            return response.getTradeStatus();
        }
        String status = response.getTradeStatus();
        if (TRADE_SUCCESS.equals(status) || TRADE_FINISHED.equals(status)) {
            // 主动查询同样必须核对金额与商户订单号，不能只凭交易状态就改本地订单。
            BigDecimal paidAmount = parseAmount(response.getTotalAmount());
            String responseOutTradeNo = StrUtil.isNotEmpty(response.getOutTradeNo())
                    ? response.getOutTradeNo() : outTradeNo;
            if (paidAmount == null || StrUtil.isBlank(responseOutTradeNo)) {
                log.warn("主动查询缺少金额或订单号，跳过：outTradeNo={}", responseOutTradeNo);
                return status;
            }
            PayCommand command = PayCommand.fromGateway(responseOutTradeNo, 1, paidAmount,
                    response.getTradeNo(), response.getBody());
            PayResult result = portalOrderService.paySuccessByOrderSn(command);
            log.info("主动查询支付结果：orderSn={}, status={}, localResult={}",
                    responseOutTradeNo, status, result.getStatus());
        }
        return status;
    }

    @Override
    public String webPay(AliPayParam aliPayParam) {
        AlipayTradeWapPayRequest request = new AlipayTradeWapPayRequest();
        fillCallbackUrls(request::setNotifyUrl, request::setReturnUrl);
        JSONObject bizContent = new JSONObject();
        bizContent.put("out_trade_no", aliPayParam.getOutTradeNo());
        BigDecimal totalAmount = requirePayableAmount(aliPayParam.getOutTradeNo());
        bizContent.put("total_amount", totalAmount);
        bizContent.put("subject", aliPayParam.getSubject());
        bizContent.put("product_code", "QUICK_WAP_WAY");
        request.setBizContent(bizContent.toString());
        try {
            return alipayClient.pageExecute(request).getBody();
        } catch (AlipayApiException e) {
            log.error("发起手机网站支付异常", e);
            return null;
        }
    }

    // ---------------------------------------------------------------------

    private void fillCallbackUrls(java.util.function.Consumer<String> notifySetter,
                                  java.util.function.Consumer<String> returnSetter) {
        if (StrUtil.isNotEmpty(alipayConfig.getNotifyUrl())) {
            notifySetter.accept(alipayConfig.getNotifyUrl());
        }
        if (StrUtil.isNotEmpty(alipayConfig.getReturnUrl())) {
            returnSetter.accept(alipayConfig.getReturnUrl());
        }
    }

    /** 发起支付：金额必须来自数据库订单，且订单必须仍是待付款。 */
    private BigDecimal requirePayableAmount(String outTradeNo) {
        OmsOrder order = portalOrderService.getOrderByOrderSn(outTradeNo);
        if (order == null || order.getPayAmount() == null) {
            Asserts.fail("订单信息或金额不能为空！");
        }
        if (order.getStatus() == null || order.getStatus() != 0) {
            Asserts.fail("订单不是待付款状态，不能发起支付！");
        }
        if (order.getPayAmount().compareTo(BigDecimal.ZERO) <= 0) {
            Asserts.fail("订单应付金额异常！");
        }
        return order.getPayAmount();
    }

    private BigDecimal parseAmount(String amount) {
        if (StrUtil.isBlank(amount)) {
            return null;
        }
        try {
            return new BigDecimal(amount.trim());
        } catch (NumberFormatException e) {
            log.warn("无法解析支付金额：{}", amount);
            return null;
        }
    }
}
