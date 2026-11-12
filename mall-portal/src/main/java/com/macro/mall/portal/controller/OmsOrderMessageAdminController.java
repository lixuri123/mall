package com.macro.mall.portal.controller;

import com.macro.mall.common.api.CommonResult;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.service.OmsOrderMessageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * B 阶段：本地消息表运维入口。
 *
 * <p>默认不注册；仅在 {@code mall.order.outbox.admin-enabled=true} 的隔离/运维环境开启，
 * 避免把「人工重试 FAILED 消息」直接暴露给普通用户。</p>
 */
@RestController
@ConditionalOnProperty(prefix = "mall.order.outbox", name = "admin-enabled",
        havingValue = "true", matchIfMissing = false)
@RequestMapping("/order/message")
@Tag(name = "OmsOrderMessageAdminController", description = "订单超时取消消息运维")
public class OmsOrderMessageAdminController {

    @Autowired
    private OmsOrderMessageService messageService;

    @Operation(summary = "查询消息链路指标")
    @GetMapping("/metrics")
    public CommonResult<Map<String, Object>> metrics() {
        return CommonResult.success(messageService.metrics());
    }

    @Operation(summary = "查询 FAILED 消息")
    @GetMapping("/failed")
    public CommonResult<List<OmsOrderMessage>> failed(
            @RequestParam(defaultValue = "50") int limit) {
        return CommonResult.success(messageService.listFailed(Math.min(limit, 200)));
    }

    @Operation(summary = "按 eventId 查询消息")
    @GetMapping("/{eventId}")
    public CommonResult<OmsOrderMessage> detail(@PathVariable String eventId) {
        return CommonResult.success(messageService.findByEventId(eventId));
    }

    @Operation(summary = "人工重试一条 FAILED 消息")
    @PostMapping("/{eventId}/retry")
    public CommonResult<Map<String, Object>> retry(@PathVariable String eventId) {
        boolean ok = messageService.manualRetry(eventId);
        Map<String, Object> result = new HashMap<>();
        result.put("eventId", eventId);
        result.put("reset", ok);
        return ok ? CommonResult.success(result) : CommonResult.failed("消息不存在或不是 FAILED 状态");
    }

    @Operation(summary = "消息体结构示例（用于人工重放时对照）")
    @GetMapping("/sample")
    public CommonResult<OrderCancelMessage> sample(@RequestParam long orderId) {
        return CommonResult.success(OrderCancelMessage.of(orderId, new java.util.Date()));
    }
}
