<template>
  <div id="app">
    <el-container>
      <el-header>
        <div class="header-content">
          <h1 class="header-title">
            <el-icon><Monitor /></el-icon>
            电影数据分析系统
          </h1>
          
          <!-- 导航菜单 -->
          <el-menu 
            :default-active="activeMenu" 
            mode="horizontal" 
            @select="handleMenuSelect"
            class="nav-menu"
          >
            <el-menu-item index="batch">
              <el-icon><DataAnalysis /></el-icon>
              <span>批处理监控</span>
            </el-menu-item>
            <el-menu-item index="hotmovie">
              <el-icon><TrendCharts /></el-icon>
              <span>热门电影排行榜</span>
            </el-menu-item>
          </el-menu>
        </div>
      </el-header>
      
      <el-main>
        <!-- 根据选中的菜单项显示不同的组件 -->
        <MovieRatingProgress v-if="activeMenu === 'batch'" />
        <HotMovieRanking v-if="activeMenu === 'hotmovie'" />
      </el-main>
    </el-container>
  </div>
</template>

<script>
import { ref } from 'vue'
import { 
  Monitor, 
  DataAnalysis, 
  TrendCharts 
} from '@element-plus/icons-vue'
import MovieRatingProgress from './components/MovieRatingProgress.vue'
import HotMovieRanking from './components/HotMovieRanking.vue'

export default {
  name: 'App',
  components: {
    MovieRatingProgress,
    HotMovieRanking
  },
  setup() {
    const activeMenu = ref('hotmovie') // 默认显示热门电影排行榜

    // 处理菜单选择
    const handleMenuSelect = (key) => {
      activeMenu.value = key
    }

    return {
      activeMenu,
      handleMenuSelect
    }
  }
}
</script>

<style scoped>
#app {
  min-height: 100vh;
  background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
}

.el-header {
  background-color: #fff;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
  padding: 0 20px;
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  height: 100%;
}

.header-title {
  color: #409EFF;
  margin: 0;
  font-size: 24px;
  display: flex;
  align-items: center;
  gap: 10px;
}

.nav-menu {
  background-color: transparent;
  border-bottom: none;
}

.nav-menu .el-menu-item {
  color: #606266;
  font-weight: 500;
}

.nav-menu .el-menu-item:hover {
  color: #409EFF;
}

.nav-menu .el-menu-item.is-active {
  color: #409EFF;
  border-bottom-color: #409EFF;
}

.el-main {
  padding: 0;
}
</style>