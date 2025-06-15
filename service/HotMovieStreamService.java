package com.abc.service;

import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 热门电影实时计算服务
 * 基于内存计算，处理前端传递的模拟数据，计算最近10分钟评分次数最多的10部电影
 */
@Service
@EnableAsync
public class HotMovieStreamService {

    // 存储热门电影数据，供前端查询
    private static final Map<String, List<HotMovie>> hotMoviesCache = new ConcurrentHashMap<>();
    private static final String HOT_MOVIES_KEY = "current_hot_movies";
    
    // 存储评分数据（最近10分钟）
    private static final List<RatingData> ratingDataList = new CopyOnWriteArrayList<>();
    
    // 窗口大小（毫秒）- 10分钟
    private static final long WINDOW_SIZE_MS = 10 * 60 * 1000L;
    
    // 计算服务运行状态
    private volatile boolean isRunning = false;

    /**
     * 评分数据结构
     */
    public static class RatingData {
        private Long userId;
        private Long movieId;
        private Double rating;
        private Long timestamp;

        public RatingData() {}

        public RatingData(Long userId, Long movieId, Double rating, Long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        // Getters and Setters
        public Long getUserId() { return userId; }
        public void setUserId(Long userId) { this.userId = userId; }
        public Long getMovieId() { return movieId; }
        public void setMovieId(Long movieId) { this.movieId = movieId; }
        public Double getRating() { return rating; }
        public void setRating(Double rating) { this.rating = rating; }
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    }

    /**
     * 热门电影数据结构
     */
    public static class HotMovie {
        private Long movieId;
        private Long ratingCount;
        private Double avgRating;
        private Long timestamp;

        public HotMovie() {}

        public HotMovie(Long movieId, Long ratingCount, Double avgRating, Long timestamp) {
            this.movieId = movieId;
            this.ratingCount = ratingCount;
            this.avgRating = avgRating;
            this.timestamp = timestamp;
        }

        // Getters and Setters
        public Long getMovieId() { return movieId; }
        public void setMovieId(Long movieId) { this.movieId = movieId; }
        public Long getRatingCount() { return ratingCount; }
        public void setRatingCount(Long ratingCount) { this.ratingCount = ratingCount; }
        public Double getAvgRating() { return avgRating; }
        public void setAvgRating(Double avgRating) { this.avgRating = avgRating; }
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    }

    /**
     * 启动热门电影实时计算服务
     */
    @Async
    public void startHotMovieStream() {
        if (isRunning) {
            System.out.println("热门电影计算服务已在运行中...");
            return;
        }
        
        isRunning = true;
        System.out.println("热门电影实时计算服务已启动...");
        
        // 启动定时计算任务（每30秒计算一次）
        new Thread(() -> {
            while (isRunning) {
                try {
                    calculateHotMovies();
                    Thread.sleep(30000); // 30秒计算一次
                } catch (InterruptedException e) {
                    System.out.println("热门电影计算服务已停止");
                    break;
                } catch (Exception e) {
                    System.err.println("热门电影计算出错: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }).start();
    }
    
    /**
     * 停止热门电影实时计算服务
     */
    public void stopHotMovieStream() {
        isRunning = false;
        System.out.println("热门电影实时计算服务已停止");
    }
    
    /**
     * 获取服务运行状态
     */
    public boolean isStreamRunning() {
        return isRunning;
    }
    
    /**
     * 添加评分数据
     */
    public void addRatingData(Long userId, Long movieId, Double rating) {
        long currentTime = System.currentTimeMillis();
        RatingData ratingData = new RatingData(userId, movieId, rating, currentTime);
        ratingDataList.add(ratingData);
        
        // 清理过期数据（超过10分钟的数据）
        cleanExpiredData(currentTime);
        
        System.out.printf("添加评分数据: 用户%d对电影%d评分%.1f%n", userId, movieId, rating);
    }
    
    /**
     * 批量添加评分数据
     */
    public void addRatingDataBatch(List<RatingData> ratings) {
        long currentTime = System.currentTimeMillis();
        
        for (RatingData rating : ratings) {
            if (rating.getTimestamp() == null) {
                rating.setTimestamp(currentTime);
            }
            ratingDataList.add(rating);
        }
        
        // 清理过期数据
        cleanExpiredData(currentTime);
        
        System.out.printf("批量添加%d条评分数据%n", ratings.size());
    }

    /**
     * 清理过期数据
     */
    private void cleanExpiredData(long currentTime) {
        long expireTime = currentTime - WINDOW_SIZE_MS;
        ratingDataList.removeIf(rating -> rating.getTimestamp() < expireTime);
    }
    
    /**
     * 计算热门电影
     */
    private void calculateHotMovies() {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - WINDOW_SIZE_MS;
        
        // 获取窗口内的数据
        List<RatingData> windowData = ratingDataList.stream()
                .filter(rating -> rating.getTimestamp() >= windowStart)
                .collect(Collectors.toList());
        
        if (windowData.isEmpty()) {
            System.out.println("窗口内无评分数据");
            return;
        }
        
        // 按电影ID分组统计
        Map<Long, List<RatingData>> movieRatings = windowData.stream()
                .collect(Collectors.groupingBy(RatingData::getMovieId));
        
        // 计算每部电影的统计信息
        List<HotMovie> hotMovies = new ArrayList<>();
        for (Map.Entry<Long, List<RatingData>> entry : movieRatings.entrySet()) {
            Long movieId = entry.getKey();
            List<RatingData> ratings = entry.getValue();
            
            long ratingCount = ratings.size();
            double avgRating = ratings.stream()
                    .mapToDouble(RatingData::getRating)
                    .average()
                    .orElse(0.0);
            
            hotMovies.add(new HotMovie(movieId, ratingCount, avgRating, currentTime));
        }
        
        // 按评分次数降序排序，取前10名
        List<HotMovie> top10 = hotMovies.stream()
                .sorted((a, b) -> Long.compare(b.getRatingCount(), a.getRatingCount()))
                .limit(10)
                .collect(Collectors.toList());
        
        // 更新缓存
        hotMoviesCache.put(HOT_MOVIES_KEY, new CopyOnWriteArrayList<>(top10));
        
        // 打印结果
        System.out.println("=== 热门电影排行榜 (最近10分钟) ===");
        System.out.printf("窗口数据: %d条评分, %d部电影%n", windowData.size(), movieRatings.size());
        for (int i = 0; i < top10.size(); i++) {
            HotMovie movie = top10.get(i);
            System.out.printf("第%d名: 电影ID=%d, 评分次数=%d, 平均评分=%.2f%n", 
                i + 1, movie.getMovieId(), movie.getRatingCount(), movie.getAvgRating());
        }
        System.out.println("==============================");
    }

    /**
     * 获取当前热门电影列表
     */
    public List<HotMovie> getCurrentHotMovies() {
        return hotMoviesCache.getOrDefault(HOT_MOVIES_KEY, new ArrayList<>());
    }

    /**
     * 清空热门电影缓存
     */
    public void clearHotMoviesCache() {
        hotMoviesCache.clear();
    }
}