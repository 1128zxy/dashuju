<template>
  <el-card class="job-card" shadow="hover">
    <template #header>
      <div class="job-header">
        <div class="job-info">
          <el-tag :type="getStatusType(job.jobStatus)" size="large">
            <el-icon><component :is="getStatusIcon(job.jobStatus)" /></el-icon>
            {{ getStatusText(job.jobStatus) }}
          </el-tag>
          <span class="job-id">作业ID: {{ job.jobId }}</span>
        </div>
        <div class="job-actions">
          <el-button 
            type="primary" 
            size="small" 
            @click="$emit('refresh', job.jobId)"
            :loading="isRefreshing"
          >
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
          <el-button 
            type="danger" 
            size="small" 
            @click="$emit('delete', job.jobId)"
            v-if="job.jobStatus !== 'RUNNING'"
          >
            <el-icon><Delete /></el-icon>
            删除
          </el-button>
        </div>
      </div>
    </template>

    <div class="job-content">
      <!-- 进度条 -->
      <div class="progress-section">
        <div class="progress-header">
          <span class="progress-label">总体进度</span>
          <span class="progress-percentage">{{ job.progressPercentage || '0.00%' }}</span>
        </div>
        <el-progress 
          :percentage="parseFloat(job.progressPercentage) || 0" 
          :status="getProgressStatus(job.jobStatus)"
          :stroke-width="12"
        />
      </div>

      <!-- 统计信息 -->
      <el-row :gutter="20" class="stats-row">
        <el-col :span="6">
          <div class="stat-item">
            <div class="stat-value">{{ formatNumber(job.totalRecords) }}</div>
            <div class="stat-label">总记录数</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="stat-item">
            <div class="stat-value">{{ formatNumber(job.processedRecords) }}</div>
            <div class="stat-label">已处理</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="stat-item">
            <div class="stat-value">{{ formatNumber(job.savedRecords) }}</div>
            <div class="stat-label">已保存</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="stat-item">
            <div class="stat-value">{{ job.processingSpeed || '0 记录/秒' }}</div>
            <div class="stat-label">处理速度</div>
          </div>
        </el-col>
      </el-row>

      <!-- 时间信息 -->
      <el-row :gutter="20" class="time-row">
        <el-col :span="8">
          <div class="time-item">
            <el-icon><Clock /></el-icon>
            <span>开始时间: {{ formatTime(job.startTime) }}</span>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="time-item">
            <el-icon><Timer /></el-icon>
            <span>运行时间: {{ job.runningTime || '0 秒' }}</span>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="time-item" v-if="job.estimatedRemainingTime && job.jobStatus === 'RUNNING'">
            <el-icon><AlarmClock /></el-icon>
            <span>预计剩余: {{ job.estimatedRemainingTime }}</span>
          </div>
          <div class="time-item" v-else-if="job.endTime">
            <el-icon><SuccessFilled /></el-icon>
            <span>结束时间: {{ formatTime(job.endTime) }}</span>
          </div>
        </el-col>
      </el-row>

      <!-- 最新消息 -->
      <div class="message-section" v-if="job.lastMessage">
        <el-alert 
          :title="job.lastMessage" 
          type="info" 
          :closable="false"
          show-icon
        />
      </div>
    </div>
  </el-card>
</template>

<script>
import {ref} from 'vue'

export default {
  name: 'JobProgressCard',
  props: {
    job: {
      type: Object,
      required: true
    }
  },
  emits: ['refresh', 'delete'],
  setup() {
    const isRefreshing = ref(false)

    const getStatusType = (status) => {
      const statusMap = {
        'RUNNING': 'warning',
        'COMPLETED': 'success',
        'FAILED': 'danger'
      }
      return statusMap[status] || 'info'
    }

    const getStatusIcon = (status) => {
      const iconMap = {
        'RUNNING': 'Loading',
        'COMPLETED': 'SuccessFilled',
        'FAILED': 'CircleCloseFilled'
      }
      return iconMap[status] || 'InfoFilled'
    }

    const getStatusText = (status) => {
      const textMap = {
        'RUNNING': '运行中',
        'COMPLETED': '已完成',
        'FAILED': '失败'
      }
      return textMap[status] || '未知状态'
    }

    const getProgressStatus = (status) => {
      if (status === 'COMPLETED') return 'success'
      if (status === 'FAILED') return 'exception'
      return null
    }

    const formatNumber = (num) => {
      if (num === undefined || num === null) return '0'
      return num.toLocaleString()
    }

    const formatTime = (timeStr) => {
      if (!timeStr) return '-'
      try {
        return new Date(timeStr).toLocaleString('zh-CN')
      } catch {
        return timeStr
      }
    }

    return {
      isRefreshing,
      getStatusType,
      getStatusIcon,
      getStatusText,
      getProgressStatus,
      formatNumber,
      formatTime
    }
  }
}
</script>

<style scoped>
.job-card {
  margin-bottom: 20px;
}

.job-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.job-info {
  display: flex;
  align-items: center;
  gap: 15px;
}

.job-id {
  font-family: 'Courier New', monospace;
  color: #666;
  font-size: 14px;
}

.job-actions {
  display: flex;
  gap: 10px;
}

.progress-section {
  margin-bottom: 20px;
}

.progress-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.progress-label {
  font-weight: bold;
  color: #333;
}

.progress-percentage {
  font-weight: bold;
  color: #409EFF;
  font-size: 16px;
}

.stats-row {
  margin-bottom: 20px;
}

.stat-item {
  text-align: center;
  padding: 15px;
  background: #f8f9fa;
  border-radius: 8px;
  border: 1px solid #e9ecef;
}

.stat-value {
  font-size: 20px;
  font-weight: bold;
  color: #409EFF;
  margin-bottom: 5px;
}

.stat-label {
  font-size: 12px;
  color: #666;
}

.time-row {
  margin-bottom: 15px;
}

.time-item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
  color: #666;
  padding: 8px;
  background: #f8f9fa;
  border-radius: 6px;
}

.message-section {
  margin-top: 15px;
}
</style>