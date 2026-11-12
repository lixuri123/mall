package com.macro.mall.portal.it;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 库存并发探针（A 阶段「同一测试脚本」）。
 *
 * 用纯 JDBC 直连真实 MySQL，分别复现改造前的 read-modify-write 锁库存（legacy）
 * 与改造后的 MySQL 条件更新（atomic）。两种模式使用完全相同的线程模型、参数与
 * 数据准备流程，唯一差别是执行的 SQL，因此改造前后的数据可以直接对比。
 *
 * 用法：
 *   java -cp <classes>;<mysql-connector-j.jar> com.macro.mall.portal.it.InventoryProbe \
 *        legacy <url> <user> <password> [stock] [threads] [qty]
 *   java -cp <classes>;<mysql-connector-j.jar> com.macro.mall.portal.it.InventoryProbe \
 *        atomic <url> <user> <password> [stock] [threads] [qty]
 *
 * 输出为单行 JSON，便于写入测试报告。
 */
public final class InventoryProbe {

    private static final String INSERT_SKU =
            "INSERT INTO pms_sku_stock (product_id, sku_code, price, stock, low_stock, pic, sale, " +
            "promotion_price, lock_stock, sp_data) VALUES (?, ?, 1.00, ?, 0, NULL, 0, NULL, 0, '[]')";
    private static final String SELECT_LOCK = "SELECT stock, lock_stock FROM pms_sku_stock WHERE id = ?";
    private static final String LEGACY_UPDATE = "UPDATE pms_sku_stock SET lock_stock = ? WHERE id = ?";
    private static final String ATOMIC_UPDATE =
            "UPDATE pms_sku_stock SET lock_stock = lock_stock + ? " +
            "WHERE id = ? AND ? > 0 AND stock IS NOT NULL AND lock_stock IS NOT NULL " +
            "AND stock >= 0 AND lock_stock >= 0 AND stock >= lock_stock " +
            "AND stock - lock_stock >= ?";
    private static final String FINAL_STATE = "SELECT stock, lock_stock FROM pms_sku_stock WHERE id = ?";
    private static final String CLEANUP = "DELETE FROM pms_sku_stock WHERE id = ?";

    private InventoryProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            throw new IllegalArgumentException("mode url user password [stock] [threads] [qty]");
        }
        String mode = args[0];
        String url = args[1];
        String user = args[2];
        String password = args[3];
        int stock = args.length > 4 ? Integer.parseInt(args[4]) : 10;
        int threads = args.length > 5 ? Integer.parseInt(args[5]) : 20;
        int quantity = args.length > 6 ? Integer.parseInt(args[6]) : 1;

        long skuId;
        try (Connection c = DriverManager.getConnection(url, user, password);
             PreparedStatement ps = c.prepareStatement(INSERT_SKU, Statement.RETURN_GENERATED_KEYS)) {
            ps.setLong(1, 999999L);
            ps.setString(2, "IT-PROBE-" + System.nanoTime());
            ps.setInt(3, stock);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                rs.next();
                skuId = rs.getLong(1);
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        // legacy 模式用屏障把所有线程卡在「读完之后、写之前」，稳定复现竞争窗口
        CyclicBarrier afterRead = new CyclicBarrier(threads);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        long begin = System.nanoTime();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try (Connection c = DriverManager.getConnection(url, user, password)) {
                    c.setAutoCommit(false);
                    ready.countDown();
                    start.await();
                    int updated;
                    if ("legacy".equals(mode)) {
                        int oldLock;
                        try (PreparedStatement ps = c.prepareStatement(SELECT_LOCK)) {
                            ps.setLong(1, skuId);
                            try (ResultSet rs = ps.executeQuery()) {
                                rs.next();
                                oldLock = rs.getInt("lock_stock");
                            }
                        }
                        afterRead.await(30, TimeUnit.SECONDS);
                        try (PreparedStatement ps = c.prepareStatement(LEGACY_UPDATE)) {
                            ps.setInt(1, oldLock + quantity);
                            ps.setLong(2, skuId);
                            updated = ps.executeUpdate();
                        }
                    } else if ("atomic".equals(mode)) {
                        try (PreparedStatement ps = c.prepareStatement(ATOMIC_UPDATE)) {
                            ps.setInt(1, quantity);
                            ps.setLong(2, skuId);
                            ps.setInt(3, quantity);
                            ps.setInt(4, quantity);
                            updated = ps.executeUpdate();
                        }
                    } else {
                        throw new IllegalArgumentException("unknown mode: " + mode);
                    }
                    c.commit();
                    if (updated == 1) {
                        success.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("thread error: " + e);
                }
                return null;
            }));
        }
        ready.await(30, TimeUnit.SECONDS);
        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        pool.shutdown();

        int finalStock;
        int finalLock;
        try (Connection c = DriverManager.getConnection(url, user, password);
             PreparedStatement ps = c.prepareStatement(FINAL_STATE)) {
            ps.setLong(1, skuId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                finalStock = rs.getInt("stock");
                finalLock = rs.getInt("lock_stock");
            }
            try (PreparedStatement del = c.prepareStatement(CLEANUP)) {
                del.setLong(1, skuId);
                del.executeUpdate();
            }
        }

        int lockedQuantity = success.get() * quantity;
        boolean oversold = lockedQuantity > stock || finalLock != lockedQuantity;
        System.out.println(String.format(
                "{\"mode\":\"%s\",\"mysql\":\"%s\",\"isolation\":\"%s\",\"initialStock\":%d," +
                "\"threads\":%d,\"quantityPerOrder\":%d,\"success\":%d,\"failed\":%d,\"errors\":%d," +
                "\"expectedLock\":%d,\"finalStock\":%d,\"finalLockStock\":%d," +
                "\"oversold\":%s,\"elapsedMs\":%d}",
                mode, readVersion(url, user, password), readIsolation(url, user, password),
                stock, threads, quantity, success.get(), failed.get(), errors.get(),
                lockedQuantity, finalStock, finalLock, oversold, elapsedMs));
    }

    private static String readVersion(String url, String user, String password) {
        try (Connection c = DriverManager.getConnection(url, user, password);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT VERSION()")) {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            return "unknown";
        }
    }

    private static String readIsolation(String url, String user, String password) {
        try (Connection c = DriverManager.getConnection(url, user, password);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT @@transaction_isolation")) {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            return "unknown";
        }
    }
}
