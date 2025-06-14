import axios from 'axios'

// 创建axios实例
const api = axios.create({
  baseURL: '/api/movie-rating',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// 请求拦截器
api.interceptors.request.use(
  config => {
    console.log('发送请求:', config.method?.toUpperCase(), config.url)
    return config
  },
  error => {
    console.error('请求错误:', error)
    return Promise.reject(error)
  }
)

// 响应拦截器
api.interceptors.response.use(
  response => {
    console.log('收到响应:', response.status, response.data)
    return response.data
  },
  error => {
    console.error('响应错误:', error)
    
    // 处理不同类型的错误
    if (error.response) {
      // 服务器返回错误状态码
      const { status, data } = error.response
      const message = data?.message || `服务器错误 (${status})`
      return Promise.reject(new Error(message))
    } else if (error.request) {
      // 请求发送失败
      return Promise.reject(new Error('网络连接失败，请检查网络设置'))
    } else {
      // 其他错误
      return Promise.reject(new Error(error.message || '未知错误'))
    }
  }
)

// 电影评分API接口
export const movieRatingApi = {
  /**
   * 启动批处理作业
   * @returns {Promise} 返回作业启动结果
   */
  async startProcessing() {
    try {
      const response = await api.post('/process')
      return response
    } catch (error) {
      console.error('启动批处理失败:', error)
      throw error
    }
  },

  /**
   * 获取特定作业的进度
   * @param {string} jobId 作业ID
   * @returns {Promise} 返回作业进度信息
   */
  async getJobProgress(jobId) {
    try {
      const response = await api.get(`/progress/${jobId}`)
      return response
    } catch (error) {
      console.error(`获取作业 ${jobId} 进度失败:`, error)
      throw error
    }
  },

  /**
   * 获取所有作业的进度
   * @returns {Promise} 返回所有作业进度信息
   */
  async getAllProgress() {
    try {
      const response = await api.get('/progress')
      return response
    } catch (error) {
      console.error('获取所有作业进度失败:', error)
      throw error
    }
  },

  /**
   * 获取系统状态
   * @returns {Promise} 返回系统状态信息
   */
  async getSystemStatus() {
    try {
      const response = await api.get('/status')
      return response
    } catch (error) {
      console.error('获取系统状态失败:', error)
      throw error
    }
  }
}

export default api