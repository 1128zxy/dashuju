package com.abc.controller;

import com.abc.service.HotMovieStreamService;
import com.abc.service.RatingDataGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 热门电影实时计算控制器
 * 提供热门电影相关的REST API接口
 */
@RestController
@RequestMapping("/api/hot-movies")
@CrossOrigin(origins = "*")
public class HotMovieController {

    @Autowired
    private HotMovieStreamService hotMovieStreamService;

    @Autowired
    private RatingDataGeneratorService ratingDataGeneratorService;

    /**
     * 启动热门电影流计算
     */
    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startHotMovieStream() {
        Map<String, Object> response = new HashMap<>();
        try {
            hotMovieStreamService.startHotMovieStream();
            response.put("success", true);
            response.put("message", "热门电影计算已启动");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "启动失败: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    /**
     * 停止热门电影流计算
     */
    @PostMapping("/stop")
    public ResponseEntity<Map<String, Object>> stopHotMovieStream() {
        Map<String, Object> response = new HashMap<>();
        try {
            hotMovieStreamService.stopHotMovieStream();
            response.put("success", true);
            response.put("message", "热门电影计算已停止");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "停止失败: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    /**
     * 检查热门电影计算状态
     */
    @GetMapping("/status1")
    public ResponseEntity<Map<String, Object>> getStreamStatus() {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("isRunning", hotMovieStreamService.isStreamRunning());
        return ResponseEntity.ok(response);
    }

    /**
     * 获取当前热门电影列表
     */
    @GetMapping("/current")
    public ResponseEntity<Map<String, Object>> getCurrentHotMovies() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            List<HotMovieStreamService.HotMovie> hotMovies = hotMovieStreamService.getCurrentHotMovies();
            
            response.put("status", "success");
            response.put("data", hotMovies);
            response.put("count", hotMovies.size());
            response.put("timestamp", System.currentTimeMillis());
            response.put("description", "最近10分钟评分次数最多的电影排行榜");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "获取热门电影列表失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 清空热门电影缓存
     */
    @DeleteMapping("/cache")
    public ResponseEntity<Map<String, Object>> clearHotMoviesCache() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            hotMovieStreamService.clearHotMoviesCache();
            
            response.put("status", "success");
            response.put("message", "热门电影缓存已清空");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "清空缓存失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 启动实时数据生成器
     */
    @PostMapping("/generator/start")
    public ResponseEntity<Map<String, Object>> startDataGenerator() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            ratingDataGeneratorService.startGenerator();
            
            response.put("status", "success");
            response.put("message", "实时评分数据生成器已启动");
            response.put("description", "正在模拟用户实时评分数据");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "启动数据生成器失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 停止实时数据生成器
     */
    @PostMapping("/generator/stop")
    public ResponseEntity<Map<String, Object>> stopDataGenerator() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            ratingDataGeneratorService.stopGenerator();
            
            response.put("status", "success");
            response.put("message", "实时评分数据生成器已停止");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "停止数据生成器失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 获取数据生成器状态
     */
    @GetMapping("/generator/status")
    public ResponseEntity<Map<String, Object>> getGeneratorStatus() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            Map<String, Object> status = ratingDataGeneratorService.getGeneratorStatus();
            
            response.put("status", "success");
            response.put("data", status);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "获取生成器状态失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 设置数据生成速率
     */
    @PutMapping("/generator/rate")
    public ResponseEntity<Map<String, Object>> setGenerationRate(@RequestParam int rate) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            ratingDataGeneratorService.setGenerationRate(rate);
            
            response.put("status", "success");
            response.put("message", "数据生成速率已更新为: " + rate + " 条/秒");
            response.put("newRate", rate);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "设置生成速率失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 重置数据生成计数器
     */
    @PostMapping("/generator/reset")
    public ResponseEntity<Map<String, Object>> resetGeneratorCounter() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            ratingDataGeneratorService.resetCounter();
            
            response.put("status", "success");
            response.put("message", "数据生成计数器已重置");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "重置计数器失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 获取热门电影系统状态
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            List<HotMovieStreamService.HotMovie> hotMovies = hotMovieStreamService.getCurrentHotMovies();
            Map<String, Object> generatorStatus = ratingDataGeneratorService.getGeneratorStatus();
            
            response.put("status", "success");
            response.put("message", "热门电影实时计算系统运行中");
            response.put("hotMoviesCount", hotMovies.size());
            response.put("generatorStatus", generatorStatus);
            response.put("features", new String[]{
                "实时计算最近10分钟热门电影",
                "支持模拟用户评分数据生成",
                "基于Flink流处理引擎",
                "支持动态调整数据生成速率"
            });
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "获取系统状态失败");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}