package com.abc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class ProgressTrackingService {

    // 存储不同作业的进度信息
    private final ConcurrentHashMap<String, JobProgress> jobProgressMap = new ConcurrentHashMap<>();

    /**
     * 创建新的作业进度跟踪
     *
     * @param jobId        作业ID
     * @param totalRecords 总记录数（如果已知）
     * @param description  作业描述
     */
    public void createJob(String jobId, long totalRecords, String description) {
        JobProgress progress = new JobProgress();
        progress.jobId = jobId;
        progress.totalRecords = totalRecords;
        progress.description = description;
        progress.status = JobStatus.RUNNING;
        progress.startTime = LocalDateTime.now();
        progress.lastUpdateTime = LocalDateTime.now();

        jobProgressMap.put(jobId, progress);
    }

    /**
     * 更新作业进度
     *
     * @param jobId            作业ID
     * @param processedRecords 已处理记录数
     * @param message          进度消息
     */
    public void updateProgress(String jobId, long processedRecords, String message) {
        JobProgress progress = jobProgressMap.get(jobId);
        if (progress != null) {
            progress.processedRecords.set(processedRecords);
            progress.lastMessage = message;
            progress.lastUpdateTime = LocalDateTime.now();

            // 计算进度百分比
            if (progress.totalRecords > 0) {
                progress.progressPercentage = (double) processedRecords / progress.totalRecords * 100;
            }
        }
    }

    /**
     * 更新总记录数
     *
     * @param jobId        作业ID
     * @param totalRecords 总记录数
     */
    public void updateTotalRecords(String jobId, long totalRecords) {
        JobProgress progress = jobProgressMap.get(jobId);
        if (progress != null) {
            progress.totalRecords = totalRecords;
            progress.lastUpdateTime = LocalDateTime.now();
            progress.lastMessage = "总记录数: " + totalRecords;
        }
    }

    /**
     * 更新HBase保存进度
     *
     * @param jobId        作业ID
     * @param savedRecords 已保存记录数
     */
    public void updateSavedProgress(String jobId, long savedRecords) {
        JobProgress progress = jobProgressMap.get(jobId);
        if (progress != null) {
            progress.savedRecords.set(savedRecords);
            progress.lastUpdateTime = LocalDateTime.now();
            progress.lastMessage = "已保存 " + savedRecords + " 条记录到HBase";
        }
    }

    /**
     * 标记作业完成
     *
     * @param jobId   作业ID
     * @param success 是否成功
     * @param message 完成消息
     */
    public void completeJob(String jobId, boolean success, String message) {
        JobProgress progress = jobProgressMap.get(jobId);
        if (progress != null) {
            progress.status = success ? JobStatus.COMPLETED : JobStatus.FAILED;
            progress.endTime = LocalDateTime.now();
            progress.lastMessage = message;
            progress.lastUpdateTime = LocalDateTime.now();
        }
    }

    /**
     * 获取作业进度
     *
     * @param jobId 作业ID
     * @return 作业进度信息
     */
    public JobProgress getJobProgress(String jobId) {
        return jobProgressMap.get(jobId);
    }

    /**
     * 获取所有作业进度
     *
     * @return 所有作业进度信息
     */
    public ConcurrentHashMap<String, JobProgress> getAllJobProgress() {
        return new ConcurrentHashMap<>(jobProgressMap);
    }


    /**
     * 清理已完成的作业（可选，用于内存管理）
     */
    public void cleanupCompletedJobs() {
        jobProgressMap.entrySet().removeIf(entry ->
                entry.getValue().status == JobStatus.COMPLETED ||
                        entry.getValue().status == JobStatus.FAILED
        );
    }

    /**
     * 作业进度信息类
     */
    public static class JobProgress {
        public String jobId;
        public String description;
        public JobStatus status;
        public long totalRecords;
        public AtomicLong processedRecords = new AtomicLong(0);
        public AtomicLong savedRecords = new AtomicLong(0);
        public double progressPercentage = 0.0;
        public String lastMessage = "";
        public LocalDateTime startTime;
        public LocalDateTime endTime;
        public LocalDateTime lastUpdateTime;

        // 计算处理速度（记录/秒）
        public double getProcessingSpeed() {
            if (startTime == null || lastUpdateTime == null) {
                return 0.0;
            }

            long seconds = java.time.Duration.between(startTime, lastUpdateTime).getSeconds();
            if (seconds <= 0) {
                return 0.0;
            }

            return (double) processedRecords.get() / seconds;
        }

        // 估算剩余时间（秒）
        public long getEstimatedRemainingSeconds() {
            if (totalRecords <= 0 || processedRecords.get() <= 0) {
                return -1; // 无法估算
            }

            double speed = getProcessingSpeed();
            if (speed <= 0) {
                return -1;
            }

            long remaining = totalRecords - processedRecords.get();
            return (long) (remaining / speed);
        }

        // 获取运行时长（秒）
        public long getRunningSeconds() {
            if (startTime == null) {
                return 0;
            }

            LocalDateTime endTimeToUse = endTime != null ? endTime : LocalDateTime.now();
            return java.time.Duration.between(startTime, endTimeToUse).getSeconds();
        }

        // 格式化时间
        public String getFormattedStartTime() {
            return startTime != null ? startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "";
        }

        public String getFormattedEndTime() {
            return endTime != null ? endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "";
        }

        public String getFormattedLastUpdateTime() {
            return lastUpdateTime != null ? lastUpdateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "";
        }
    }

    /**
     * 作业状态枚举
     */
    public enum JobStatus {
        RUNNING("运行中"),
        COMPLETED("已完成"),
        FAILED("失败"),
        CANCELLED("已取消");

        private final String description;

        JobStatus(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }
}