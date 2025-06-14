<template>
  <div class="hot-movie-ranking">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1 class="title">
        <el-icon><TrendCharts /></el-icon>
        热门电影实时排行榜
      </h1>
      <p class="subtitle">基于最近10分钟用户评分次数的实时统计</p>
    </div>

    <!-- 控制面板 -->
    <el-card class="control-panel" shadow="hover">
      <template #header>
        <div class="card-header">
          <span><el-icon><Setting /></el-icon> 控制面板</span>
        </div>
      </template>
      
      <div class="control-buttons">
        <el-button 
          type="primary" 
          :icon="VideoPlay" 
          @click="startHotMovieStream"
          :loading="streamLoading"
          size="large">
          启动实时计算
        </el-button>
        
        <el-button 
          type="danger" 
          :icon="VideoPause" 
          @click="stopHotMovieStream"
          :loading="streamLoading"
          size="large">
          停止实时计算
        </el-button>
        
        <el-button 
          type="success" 
          :icon="DataAnalysis" 
          @click="startDataGenerator"
          :loading="generatorLoading"
          size="large">
          启动数据生成
        </el-button>
        
        <el-button 
          type="warning" 
          :icon="VideoPause" 
          @click="stopDataGenerator"
          size="large">
          停止数据生成
        </el-button>
        
        <el-button 
          type="info" 
          :icon="Refresh" 
          @click="refreshHotMovies"
          :loading="refreshLoading"
          size="large">
          刷新排行榜
        </el-button>
      </div>

      <!-- 生成器状态 -->
      <div class="generator-status" v-if="generatorStatus">
        <el-divider content-position="left">数据生成器状态</el-divider>
        <el-row :gutter="20">
          <el-col :span="6">
            <el-statistic title="运行状态" :value="generatorStatus.isRunning ? '运行中' : '已停止'">
              <template #suffix>
                <el-icon :color="generatorStatus.isRunning ? '#67C23A' : '#F56C6C'">
                  <component :is="generatorStatus.isRunning ? 'VideoPlay' : 'VideoPause'" />
                </el-icon>
              </template>
            </el-statistic>
          </el-col>
          <el-col :span="6">
            <el-statistic title="已生成数据" :value="generatorStatus.generatedCount" suffix="条" />
          </el-col>
          <el-col :span="6">
            <el-statistic title="生成速率" :value="generatorStatus.generationRate" suffix="条/秒" />
          </el-col>
          <el-col :span="6">
            <div class="rate-control">
              <el-input-number 
                v-model="newRate" 
                :min="1" 
                :max="100" 
                size="small"
                controls-position="right" />
              <el-button 
                type="primary" 
                size="small" 
                @click="updateGenerationRate"
                style="margin-left: 8px;">
                更新速率
              </el-button>
            </div>
          </el-col>
        </el-row>
      </div>
    </el-card>

    <!-- 热门电影排行榜 -->
    <el-card class="ranking-panel" shadow="hover">
      <template #header>
        <div class="card-header">
          <span><el-icon><Trophy /></el-icon> 热门电影排行榜</span>
          <div class="header-actions">
            <el-tag :type="hotMovies.length > 0 ? 'success' : 'info'">
              {{ hotMovies.length > 0 ? `共${hotMovies.length}部电影` : '暂无数据' }}
            </el-tag>
            <el-button 
              type="text" 
              :icon="Refresh" 
              @click="refreshHotMovies"
              :loading="refreshLoading">
              刷新
            </el-button>
          </div>
        </div>
      </template>

      <!-- 排行榜列表 -->
      <div v-if="hotMovies.length > 0" class="ranking-list">
        <div 
          v-for="(movie, index) in hotMovies" 
          :key="movie.movieId"
          class="ranking-item"
          :class="getRankingClass(index)">
          
          <!-- 排名 -->
          <div class="rank">
            <div class="rank-number" :class="getRankNumberClass(index)">
              {{ index + 1 }}
            </div>
            <el-icon v-if="index < 3" class="rank-icon" :color="getRankIconColor(index)">
              <component :is="getRankIcon(index)" />
            </el-icon>
          </div>

          <!-- 电影信息 -->
          <div class="movie-info">
            <div class="movie-title">
              <span class="movie-id">电影 ID: {{ movie.movieId }}</span>
              <el-tag size="small" type="primary">热门</el-tag>
            </div>
            <div class="movie-stats">
              <el-row :gutter="20">
                <el-col :span="8">
                  <div class="stat-item">
                    <el-icon><Star /></el-icon>
                    <span>评分次数: {{ movie.ratingCount }}</span>
                  </div>
                </el-col>
                <el-col :span="8">
                  <div class="stat-item">
                    <el-icon><Medal /></el-icon>
                    <span>平均评分: {{ movie.avgRating?.toFixed(1) }}</span>
                  </div>
                </el-col>
                <el-col :span="8">
                  <div class="stat-item">
                    <el-icon><Clock /></el-icon>
                    <span>更新时间: {{ formatTime(movie.timestamp) }}</span>
                  </div>
                </el-col>
              </el-row>
            </div>
          </div>

          <!-- 评分进度条 -->
          <div class="rating-progress">
            <el-progress 
              :percentage="(movie.avgRating / 5) * 100" 
              :color="getProgressColor(movie.avgRating)"
              :stroke-width="8"
              :show-text="false" />
          </div>
        </div>
      </div>

      <!-- 空状态 -->
      <el-empty v-else description="暂无热门电影数据" :image-size="120">
        <el-button type="primary" @click="startDataGenerator">开始生成数据</el-button>
      </el-empty>
    </el-card>

    <!-- 自动刷新提示 -->
    <div class="auto-refresh-info">
      <el-alert 
        title="自动刷新" 
        type="info" 
        :closable="false"
        show-icon>
        <template #default>
          排行榜每 {{ refreshInterval / 1000 }} 秒自动刷新一次，展示最新的热门电影数据
        </template>
      </el-alert>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElNotification } from 'element-plus'
import { 
  TrendCharts, 
  Setting, 
  VideoPlay, 
  VideoPause, 
  DataAnalysis, 
  Refresh, 
  Trophy, 
  Star, 
  Medal, 
  Clock 
} from '@element-plus/icons-vue'
import { hotMovieApi } from '../api/hotMovie.js'

// 响应式数据
const hotMovies = ref([])
const generatorStatus = ref(null)
const streamLoading = ref(false)
const generatorLoading = ref(false)
const refreshLoading = ref(false)
const newRate = ref(10)
const refreshInterval = ref(5000) // 5秒刷新一次
let refreshTimer = null

// 启动热门电影实时计算
const startHotMovieStream = async () => {
  streamLoading.value = true
  try {
    const response = await hotMovieApi.startHotMovieStream()
    ElMessage.success(response.message)
    ElNotification({
      title: '计算启动成功',
      message: '热门电影实时计算已启动',
      type: 'success'
    })
  } catch (error) {
    ElMessage.error('启动计算失败: ' + error.message)
  } finally {
    streamLoading.value = false
  }
}

// 停止热门电影实时计算
const stopHotMovieStream = async () => {
  streamLoading.value = true
  try {
    const response = await hotMovieApi.stopHotMovieStream()
    ElMessage.success(response.message)
    ElNotification({
      title: '计算停止成功',
      message: '热门电影实时计算已停止',
      type: 'warning'
    })
  } catch (error) {
    ElMessage.error('停止计算失败: ' + error.message)
  } finally {
    streamLoading.value = false
  }
}

// 启动数据生成器
const startDataGenerator = async () => {
  generatorLoading.value = true
  try {
    const response = await hotMovieApi.startDataGenerator()
    ElMessage.success(response.message)
    await getGeneratorStatus()
  } catch (error) {
    ElMessage.error('启动数据生成器失败: ' + error.message)
  } finally {
    generatorLoading.value = false
  }
}

// 停止数据生成器
const stopDataGenerator = async () => {
  try {
    const response = await hotMovieApi.stopDataGenerator()
    ElMessage.success(response.message)
    await getGeneratorStatus()
  } catch (error) {
    ElMessage.error('停止数据生成器失败: ' + error.message)
  }
}

// 刷新热门电影列表
const refreshHotMovies = async () => {
  refreshLoading.value = true
  try {
    const response = await hotMovieApi.getCurrentHotMovies()
    hotMovies.value = response.data || []
  } catch (error) {
    ElMessage.error('刷新热门电影列表失败: ' + error.message)
  } finally {
    refreshLoading.value = false
  }
}

// 获取生成器状态
const getGeneratorStatus = async () => {
  try {
    const response = await hotMovieApi.getGeneratorStatus()
    generatorStatus.value = response.data
    newRate.value = response.data.generationRate
  } catch (error) {
    console.error('获取生成器状态失败:', error)
  }
}

// 更新生成速率
const updateGenerationRate = async () => {
  try {
    const response = await hotMovieApi.setGenerationRate(newRate.value)
    ElMessage.success(response.message)
    await getGeneratorStatus()
  } catch (error) {
    ElMessage.error('更新生成速率失败: ' + error.message)
  }
}

// 获取排名样式类
const getRankingClass = (index) => {
  if (index === 0) return 'rank-first'
  if (index === 1) return 'rank-second'
  if (index === 2) return 'rank-third'
  return ''
}

// 获取排名数字样式类
const getRankNumberClass = (index) => {
  if (index < 3) return 'top-three'
  return ''
}

// 获取排名图标
const getRankIcon = (index) => {
  const icons = ['Trophy', 'Medal', 'Star']
  return icons[index] || 'Star'
}

// 获取排名图标颜色
const getRankIconColor = (index) => {
  const colors = ['#FFD700', '#C0C0C0', '#CD7F32']
  return colors[index] || '#409EFF'
}

// 获取进度条颜色
const getProgressColor = (rating) => {
  if (rating >= 4.5) return '#67C23A'
  if (rating >= 4.0) return '#E6A23C'
  if (rating >= 3.5) return '#F56C6C'
  return '#909399'
}

// 格式化时间
const formatTime = (timestamp) => {
  if (!timestamp) return '-'
  const date = new Date(timestamp)
  return date.toLocaleTimeString()
}

// 启动自动刷新
const startAutoRefresh = () => {
  refreshTimer = setInterval(() => {
    refreshHotMovies()
    getGeneratorStatus()
  }, refreshInterval.value)
}

// 停止自动刷新
const stopAutoRefresh = () => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

// 组件挂载
onMounted(() => {
  refreshHotMovies()
  getGeneratorStatus()
  startAutoRefresh()
})

// 组件卸载
onUnmounted(() => {
  stopAutoRefresh()
})
</script>

<style scoped>
.hot-movie-ranking {
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

.page-header {
  text-align: center;
  margin-bottom: 30px;
}

.title {
  font-size: 2.5em;
  color: #409EFF;
  margin-bottom: 10px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
}

.subtitle {
  font-size: 1.1em;
  color: #666;
  margin: 0;
}

.control-panel {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: bold;
}

.control-buttons {
  display: flex;
  gap: 15px;
  flex-wrap: wrap;
  margin-bottom: 20px;
}

.generator-status {
  margin-top: 20px;
}

.rate-control {
  display: flex;
  align-items: center;
}

.ranking-panel {
  margin-bottom: 20px;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.ranking-list {
  display: flex;
  flex-direction: column;
  gap: 15px;
}

.ranking-item {
  display: flex;
  align-items: center;
  padding: 20px;
  border: 2px solid #f0f0f0;
  border-radius: 12px;
  transition: all 0.3s ease;
  background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
}

.ranking-item:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
}

.rank-first {
  border-color: #FFD700;
  background: linear-gradient(135deg, #fff9e6 0%, #ffffff 100%);
}

.rank-second {
  border-color: #C0C0C0;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
}

.rank-third {
  border-color: #CD7F32;
  background: linear-gradient(135deg, #fdf2e9 0%, #ffffff 100%);
}

.rank {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-right: 20px;
  min-width: 60px;
}

.rank-number {
  font-size: 2em;
  font-weight: bold;
  color: #666;
  line-height: 1;
}

.rank-number.top-three {
  color: #409EFF;
}

.rank-icon {
  font-size: 1.5em;
  margin-top: 5px;
}

.movie-info {
  flex: 1;
  margin-right: 20px;
}

.movie-title {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
}

.movie-id {
  font-size: 1.2em;
  font-weight: bold;
  color: #333;
}

.movie-stats {
  color: #666;
}

.stat-item {
  display: flex;
  align-items: center;
  gap: 5px;
  font-size: 0.9em;
}

.rating-progress {
  width: 120px;
}

.auto-refresh-info {
  margin-top: 20px;
}

@media (max-width: 768px) {
  .control-buttons {
    flex-direction: column;
  }
  
  .ranking-item {
    flex-direction: column;
    text-align: center;
  }
  
  .rank {
    margin-right: 0;
    margin-bottom: 15px;
  }
  
  .movie-info {
    margin-right: 0;
    margin-bottom: 15px;
  }
  
  .rating-progress {
    width: 100%;
  }
}
</style>