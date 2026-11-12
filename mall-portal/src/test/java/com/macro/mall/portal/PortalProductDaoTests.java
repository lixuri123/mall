package com.macro.mall.portal;

import com.macro.mall.portal.dao.PortalProductDao;
import com.macro.mall.portal.domain.PromotionProduct;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by macro on 2018/8/27.
 * 前台商品查询逻辑单元测试
 */
@SpringBootTest(properties = {
        "mall.order.timeout-scan.enabled=false",
        "mall.order.outbox.dispatch.enabled=false",
        "mall.order.outbox.consume.enabled=false",
        "mall.order.outbox.metrics.enabled=false",
})
public class PortalProductDaoTests {
    @Autowired
    private PortalProductDao portalProductDao;
    @Test
    public void testGetPromotionProductList(){
        List<Long> ids = new ArrayList<>();
        ids.add(26L);
        ids.add(27L);
        ids.add(28L);
        ids.add(29L);
        List<PromotionProduct> promotionProductList = portalProductDao.getPromotionProductList(ids);
        assertEquals(4,promotionProductList.size());
    }
}
