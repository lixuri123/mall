package com.macro.mall.portal;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "mall.order.timeout-scan.enabled=false",
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "spring.rabbitmq.listener.direct.auto-startup=false"
})
public class MallPortalApplicationTests {

    @Test
    public void contextLoads() {
    }

}
