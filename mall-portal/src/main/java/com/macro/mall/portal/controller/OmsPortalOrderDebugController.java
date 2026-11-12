package com.macro.mall.portal.controller;

import com.macro.mall.common.api.CommonResult;
import com.macro.mall.portal.service.OmsPortalOrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 订单调试入口。
 *
 * <p>这两个接口会绕过「可信支付结果」和「真实订单归属上下文」，默认<strong>不注册</strong>。
 * 只有显式设置 {@code mall.order.debug-endpoints-enabled=true} 的隔离环境才会创建该
 * Controller；生产与常规本地环境调用会直接 404。</p>
 */
@RestController
@Tag(name = "OmsPortalOrderDebugController", description = "订单调试入口（默认关闭）")
@RequestMapping("/order")
@ConditionalOnProperty(prefix = "mall.order", name = "debug-endpoints-enabled", havingValue = "true")
public class OmsPortalOrderDebugController {

    @Autowired
    private OmsPortalOrderService portalOrderService;

    @Operation(summary = "[调试] 直接把订单标记为支付成功")
    @RequestMapping(value = "/paySuccess", method = RequestMethod.POST)
    public CommonResult paySuccess(@RequestParam Long orderId, @RequestParam Integer payType) {
        Integer count = portalOrderService.paySuccess(orderId, payType);
        return CommonResult.success(count, "支付成功");
    }

    @Operation(summary = "[调试] 为历史订单补齐本地超时取消消息（不再直接发 MQ）")
    @RequestMapping(value = "/cancelOrder", method = RequestMethod.POST)
    public CommonResult cancelOrder(Long orderId) {
        portalOrderService.sendDelayMessageCancelOrder(orderId);
        return CommonResult.success(null);
    }
}
