package com.macro.mall.portal.component;

import com.macro.mall.portal.dao.PortalOrderDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 启动时历史库存数据预检（只读）。
 *
 * <p>条件更新 SQL 会拒绝 NULL / 负数 / lock_stock &gt; stock 的记录。这里只做识别与告警，
 * <strong>不会自动修复</strong>：直接把异常值清零可能掩盖已有待支付订单的锁定库存。
 * 处理办法见 document/sql/mall_a_precheck.sql。</p>
 */
@Slf4j
@Component
public class StockDataPrecheckRunner implements ApplicationRunner {

    @Autowired
    private PortalOrderDao portalOrderDao;

    @Override
    public void run(ApplicationArguments args) {
        try {
            List<Map<String, Object>> anomalies = portalOrderDao.listStockAnomalies();
            if (anomalies.isEmpty()) {
                log.info("库存历史数据预检通过：未发现 NULL / 负数 / 超锁定记录");
            } else {
                log.error("库存历史数据预检发现 {} 条异常，相关 SKU 的条件更新会被拒绝，"
                                + "请执行 document/sql/mall_a_precheck.sql 人工确认来源：{}",
                        anomalies.size(), anomalies);
            }
        } catch (Exception e) {
            log.error("库存历史数据预检执行失败，不影响启动，但请尽快手工执行预检脚本", e);
        }
    }
}
