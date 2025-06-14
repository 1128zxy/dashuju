<template>
  <div class="movie-rating-progress">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1 class="title">
        <el-icon><Monitor /></el-icon>
        电影评分批处理进度监控
      </h1>
      <p class="subtitle">实时监控电影评分数据批处理作业的执行进度</p>
    </div>

    <!-- 操作区域 -->
    <el-card class="operation-card" shadow="hover">
      <template #header>
        <div class="card-header">
          <span><el-icon><Setting /></el-icon> 批处理操作</span>
        </div>
      </template>
      
      <el-row :gutter="20">
        <el-col :span="12">
          <el-button 
            type="primary" 
            size="large" 
            @click="startProcessing" 
            :loading="isProcessing"
            style="width: 100%;"
          >
            <el-icon><Play /></el-icon>
            启动批处理作业
          </el-button>
        </el-col>
        <el-col :span="12">
          <el-button 
            type="success" 
            size="large" 
            @click="refreshAllProgress" 
            :loading="isRefreshing"
            style="width: 100%;"
          >
            <el-icon><Refresh /></el-icon>
            刷新所有进度
          </el-button>
        </el-col>
      </el-row>
    </el-card>

    <!-- 进度监控区域 -->
    <el-card class="progress-card" shadow="hover">
      <template #header>
        <div class="card-header">
          <span><el-icon><DataAnalysis /></el-icon> 作业进度监控</span>
          <el-tag v-if="jobList.length > 0" type="info">共 {{ jobList.length }} 个作业</el-tag>
        </div>
      </template>
      
      <div v-if="jobList.length === 0" class="empty-state">
        <el-empty description="暂无作业数据">
          <el-button type="primary" @click="startProcessing">启动第一个作业</el-button>
        </el-empty>
      </div>
      
      <div v-else>
        <el-row :gutter="20">
          <el-col :span="24" v-for="job in jobList" :key="job.jobId">
            <JobProgressCard 
              :job="job" 
              @refresh="refreshJobProgress"
              @delete="deleteJob"
            />
          </el-col>
        </el-row>
      </div>
    </el-card>
  </div>
</template>

<script>
import { onMounted, onUnmounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { 
  Monitor, 
  Setting, 
  Refresh, 
  DataAnalysis 
} from '@element-plus/icons-vue'
import JobProgressCard from './JobProgressCard.vue'
import { movieRatingApi } from '../api/movieRating.js'

export default {
  name: 'MovieRatingProgress',
  components: {
    JobProgressCard
  },
  setup() {
    const jobList = ref([])
    const isProcessing = ref(false)
    const isRefreshing = ref(false)
    let refreshTimer = null

    // 启动批处理作业
    const startProcessing = async () => {
      try {
        isProcessing.value = true
        const response = await movieRatingApi.startProcessing()
        
        if (response.status === 'success') {
          ElMessage.success('批处理作业启动成功！作业ID: ' + response.jobId)
          // 立即刷新进度列表
          await refreshAllProgress()
        } else {
          ElMessage.error('启动失败: ' + response.message)
        }
      } catch (error) {
        ElMessage.error('启动批处理作业失败: ' + error.message)
      } finally {
        isProcessing.value = false
      }
    }

    // 刷新所有作业进度
    const refreshAllProgress = async () => {
      try {
        isRefreshing.value = true
        const response = await movieRatingApi.getAllProgress()
        
        if (response.status === 'success') {
          jobList.value = Object.values(response.jobs).sort((a, b) => 
            new Date(b.startTime) - new Date(a.startTime)
          )
        }
      } catch (error) {
        console.error('刷新进度失败:', error)
      } finally {
        isRefreshing.value = false
      }
    }

    // 刷新单个作业进度
    const refreshJobProgress = async (jobId) => {
      try {
        const response = await movieRatingApi.getJobProgress(jobId)
        if (response.status === 'success') {
          const index = jobList.value.findIndex(job => job.jobId === jobId)
          if (index !== -1) {
            jobList.value[index] = response
          }
        }
      } catch (error) {
        console.error('刷新作业进度失败:', error)
      }
    }

    // 删除作业
    const deleteJob = async (jobId) => {
      try {
        await ElMessageBox.confirm(
          '确定要删除这个作业记录吗？',
          '确认删除',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning',
          }
        )
        
        jobList.value = jobList.value.filter(job => job.jobId !== jobId)
        ElMessage.success('作业记录已删除')
      } catch {
        // 用户取消删除
      }
    }

    // 自动刷新
    const startAutoRefresh = () => {
      refreshTimer = setInterval(() => {
        if (jobList.value.some(job => job.jobStatus === 'RUNNING')) {
          refreshAllProgress()
        }
      }, 3000) // 每3秒刷新一次
    }

    const stopAutoRefresh = () => {
      if (refreshTimer) {
        clearInterval(refreshTimer)
        refreshTimer = null
      }
    }

    onMounted(() => {
      refreshAllProgress()
      startAutoRefresh()
    })

    onUnmounted(() => {
      stopAutoRefresh()
    })

    return {
      jobList,
      isProcessing,
      isRefreshing,
      startProcessing,
      refreshAllProgress,
      refreshJobProgress,
      deleteJob
    }
  }
}
</script>

<style scoped>
.movie-rating-progress {
  padding: 20px;
}

.page-header {
  text-align: center;
  margin-bottom: 30px;
}

.title {
  font-size: 28px;
  color: #409EFF;
  margin: 0 0 10px 0;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
}

.subtitle {
  font-size: 16px;
  color: #666;
  margin: 0;
}

.operation-card {
  margin-bottom: 20px;
}

.progress-card {
  min-height: 400px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: bold;
  font-size: 16px;
}

.empty-state {
  text-align: center;
  padding: 60px 0;
}
</style>