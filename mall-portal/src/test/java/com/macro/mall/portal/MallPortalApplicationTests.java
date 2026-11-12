package com.macro.mall.portal;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "mall.order.timeout-scan.enabled=false",
        "mall.order.outbox.dispatch.enabled=false",
        "mall.order.outbox.consume.enabled=false",
        "mall.order.outbox.metrics.enabled=false",
})
public class MallPortalApplicationTests {

    @Test
    public void contextLoads() {
    }

}
