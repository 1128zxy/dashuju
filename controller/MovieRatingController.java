package com.abc.controller;

import com.abc.service.MovieRatingFlinkService;
import com.abc.service.ProgressTrackingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/movie-rating")
public class MovieRatingController {
    
    @Autowired
    private MovieRatingFlinkService movieRatingFlinkService;
    
    @Autowired
    private ProgressTrackingService progressTrackingService;
    
    /**
     * 处理电影评分数据并保存到HBase
     * @param csvFilePath CSV文件路径（可选，默认使用项目中的数据文件）
     * @return 处理结果
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processMovieRatings(
            @RequestParam(value = "csvFilePath", required = false) String csvFilePath) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 如果没有指定文件路径，使用默认路径
            if (csvFilePath == null || csvFilePath.trim().isEmpty()) {
                csvFilePath = "data/ml-latest/ratings.csv";
            }
            
            // 生成唯一的作业ID
            String jobId = "movie-rating-" + UUID.randomUUID().toString().substring(0, 8);
            
            // 创建进度跟踪
            progressTrackingService.createJob(jobId, 30000000L, "电影评分批处理作业 - " + csvFilePath);
            
            System.out.println("开始处理电影评分数据，作业ID: " + jobId + "，文件路径: " + csvFilePath);
            
            // 异步执行Flink作业
            final String finalCsvFilePath = csvFilePath;
            new Thread(() -> {
                try {
                    movieRatingFlinkService.processMovieRatings(finalCsvFilePath, jobId, progressTrackingService);
                    progressTrackingService.completeJob(jobId, true, "电影评分处理作业成功完成");
                } catch (Exception e) {
                    System.err.println("Flink作业执行失败: " + e.getMessage());
                    e.printStackTrace();
                    progressTrackingService.completeJob(jobId, false, "作业执行失败: " + e.getMessage());
                }
            }).start();
            
            response.put("status", "success");
            response.put("message", "电影评分处理作业已启动");
            response.put("jobId", jobId);
            response.put("csvFilePath", csvFilePath);
            response.put("description", "正在使用Flink批处理计算每部电影的平均评分并保存到HBase数据库");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "启动处理作业失败: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * 查询特定作业的处理进度
     * @param jobId 作业ID
     * @return 作业进度信息
     */
    @GetMapping("/progress/{jobId}")
    public ResponseEntity<Map<String, Object>> getJobProgress(@PathVariable String jobId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            ProgressTrackingService.JobProgress progress = progressTrackingService.getJobProgress(jobId);
            
            if (progress == null) {
                response.put("status", "error");
                response.put("message", "未找到指定的作业ID: " + jobId);
                return ResponseEntity.status(404).body(response);
            }
            
            response.put("status", "success");
            response.put("jobId", progress.jobId);
            response.put("description", progress.description);
            response.put("jobStatus", progress.status.getDescription());
            response.put("totalRecords", progress.totalRecords);
            response.put("processedRecords", progress.processedRecords.get());
            response.put("savedRecords", progress.savedRecords.get());
            response.put("progressPercentage", String.format("%.2f%%", progress.progressPercentage));
            response.put("processingSpeed", String.format("%.0f 记录/秒", progress.getProcessingSpeed()));
            response.put("runningTime", progress.getRunningSeconds() + " 秒");
            response.put("lastMessage", progress.lastMessage);
            response.put("startTime", progress.getFormattedStartTime());
            response.put("endTime", progress.getFormattedEndTime());
            response.put("lastUpdateTime", progress.getFormattedLastUpdateTime());
            
            // 估算剩余时间
            long remainingSeconds = progress.getEstimatedRemainingSeconds();
            if (remainingSeconds > 0) {
                long hours = remainingSeconds / 3600;
                long minutes = (remainingSeconds % 3600) / 60;
                long seconds = remainingSeconds % 60;
                response.put("estimatedRemainingTime", String.format("%d小时%d分钟%d秒", hours, minutes, seconds));
            } else {
                response.put("estimatedRemainingTime", "无法估算");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "查询进度失败: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * 查询所有作业的处理进度
     * @return 所有作业进度信息
     */
    @GetMapping("/progress")
    public ResponseEntity<Map<String, Object>> getAllJobProgress() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            Map<String, ProgressTrackingService.JobProgress> allProgress = progressTrackingService.getAllJobProgress();
            
            response.put("status", "success");
            response.put("totalJobs", allProgress.size());
            response.put("jobs", allProgress);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "查询所有作业进度失败: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }
    
    /**
     * 获取处理状态信息
     * @return 状态信息
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getProcessingStatus() {
        Map<String, Object> response = new HashMap<>();
        
        response.put("status", "info");
        response.put("message", "电影评分处理服务运行中");
        response.put("description", "使用Flink批处理引擎处理3千万条电影评分数据");
        response.put("features", new String[]{
            "支持大规模数据处理（3千万条记录）",
            "每1万条数据显示处理进度",
            "计算结果自动保存到HBase数据库",
            "支持自定义CSV文件路径"
        });
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * 健康检查接口
     * @return 服务状态
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Movie Rating Flink Service");
        return ResponseEntity.ok(response);
    }
}