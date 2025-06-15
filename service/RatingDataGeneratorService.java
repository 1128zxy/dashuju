package com.abc.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 评分数据生成器服务
 * 模拟实时用户评分数据并直接传递给热门电影计算服务
 */
@Service
public class RatingDataGeneratorService {

    @Autowired
    private HotMovieStreamService hotMovieStreamService;

    @Value("${data-generator.default-rate:10}")
    private int defaultGenerationRate;

    @Value("${data-generator.movie-count:100}")
    private int movieCount;

    @Value("${data-generator.user-count:1000}")
    private int userCount;

    @Value("${data-generator.hot-movie-ratio:0.3}")
    private double hotMovieRatio;

    @Value("${data-generator.rating.min:1.0}")
    private double minRating;

    @Value("${data-generator.rating.max:5.0}")
    private double maxRating;

    private ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicLong generatedCount = new AtomicLong(0);
    private final Random random = new Random();
    private int generationRate = 10;

    // 热门电影ID列表（模拟某些电影更受欢迎）
    private final List<Long> hotMovieIds = Arrays.asList(
        1L, 2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L,
        29L, 31L, 37L, 41L, 43L, 47L, 53L, 59L, 61L, 67L
    );

    /**
     * 初始化数据生成器
     */
    public void init() {
        System.out.println("评分数据生成器服务初始化完成");
        System.out.printf("配置: 电影数=%d, 用户数=%d, 热门电影比例=%.1f%%, 评分范围=[%.1f-%.1f]%n", 
            movieCount, userCount, hotMovieRatio * 100, minRating, maxRating);
    }

    /**
     * 启动数据生成器
     */
    public synchronized void startGenerator() {
        if (isRunning.get()) {
            System.out.println("数据生成器已经在运行中...");
            return;
        }

        try {
            generationRate = defaultGenerationRate;
            scheduler = Executors.newScheduledThreadPool(2);
            isRunning.set(true);
            generatedCount.set(0);

            // 启动数据生成任务
            scheduler.scheduleAtFixedRate(this::generateAndSendRating, 0, 1000 / generationRate, TimeUnit.MILLISECONDS);

            // 启动统计任务
            scheduler.scheduleAtFixedRate(this::printStatistics, 10, 10, TimeUnit.SECONDS);

            System.out.println("实时评分数据生成器已启动，生成速率: " + generationRate + " 条/秒");
        } catch (Exception e) {
            System.err.println("启动数据生成器失败: " + e.getMessage());
            e.printStackTrace();
            stopGenerator();
        }
    }

    /**
     * 停止数据生成器
     */
    public synchronized void stopGenerator() {
        if (!isRunning.get()) {
            System.out.println("数据生成器未在运行...");
            return;
        }

        isRunning.set(false);

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }



        System.out.println("实时评分数据生成器已停止，总共生成: " + generatedCount.get() + " 条数据");
    }

    /**
     * 生成并发送评分数据
     */
    private void generateAndSendRating() {
        try {
            // 生成用户ID
            long userId = random.nextInt(userCount) + 1;
            
            // 生成电影ID（偏向热门电影）
            long movieId = generateMovieId();
            
            // 生成评分（热门电影偏向高分）
            double rating = generateRating(movieId);
            
            // 直接添加到热门电影计算服务
            hotMovieStreamService.addRatingData(userId, movieId, rating);
            
            generatedCount.incrementAndGet();
            
            // 每100条数据打印一次统计
            if (generatedCount.get() % 100 == 0) {
                printStatistics();
            }
            
        } catch (Exception e) {
            System.err.println("生成评分数据失败: " + e.getMessage());
        }
    }

    /**
     * 生成电影ID（偏向热门电影）
     */
    private long generateMovieId() {
        if (random.nextDouble() < hotMovieRatio) {
            // 选择热门电影
            return hotMovieIds.get(random.nextInt(hotMovieIds.size()));
        } else {
            // 选择普通电影
            return random.nextInt(movieCount) + 1;
        }
    }

    /**
     * 生成评分（热门电影偏向高分）
     */
    private double generateRating(long movieId) {
        double rating;
        
        if (hotMovieIds.contains(movieId)) {
            // 热门电影偏向高分（均值4.2，标准差0.6）
            rating = random.nextGaussian() * 0.6 + 4.2;
        } else {
            // 普通电影正常分布（均值3.5，标准差1.0）
            rating = random.nextGaussian() * 1.0 + 3.5;
        }
        
        // 限制在配置的范围内
        rating = Math.max(minRating, Math.min(maxRating, rating));
        
        // 四舍五入到0.5
        return Math.round(rating * 2.0) / 2.0;
    }

    /**
     * 打印统计信息
     */
    private void printStatistics() {
        if (isRunning.get()) {
            long count = generatedCount.get();
            System.out.println("数据生成统计 - 总计: " + count + " 条，当前速率: " + generationRate + " 条/秒");
        }
    }

    /**
     * 获取生成器状态
     */
    public Map<String, Object> getGeneratorStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("isRunning", isRunning.get());
        status.put("generatedCount", generatedCount.get());
        status.put("generationRate", generationRate);
        status.put("movieCount", movieCount);
        status.put("userCount", userCount);
        status.put("hotMovieRatio", hotMovieRatio);
        return status;
    }

    /**
     * 设置生成速率
     */
    public void setGenerationRate(int rate) {
        this.generationRate = Math.max(1, Math.min(1000, rate)); // 限制在1-1000之间
        if (isRunning.get()) {
            // 重启生成器以应用新的速率
            stopGenerator();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            startGenerator();
        }
    }

    /**
     * 重置计数器
     */
    public void resetCounter() {
        generatedCount.set(0);
    }
}