package com.abc.service;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.abc.service.ProgressTrackingService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MovieRatingFlinkService {

    @Value("${hbase.zookeeper.quorum}")
    private String hbaseZookeeperQuorum;

    @Value("${hbase.zookeeper.port}")
    private String hbaseZookeeperPort;

    @Value("${hbase.table.name}")
    private String tableName;

    @Value("${hbase.table.column-family}")
    private String columnFamily;

    @Value("${flink.parallelism:4}")
    private int flinkParallelism;

    @Value("${data.progress-interval:10000}")
    private int progressInterval;

    public void processMovieRatings(String csvFilePath) throws Exception {
        processMovieRatings(csvFilePath, null, null);
    }

    public void processMovieRatings(String csvFilePath, String jobId, ProgressTrackingService progressTrackingService) throws Exception {
        // 创建Flink执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(flinkParallelism);

        System.out.println("开始读取CSV文件: " + csvFilePath);

        // 读取CSV文件
        DataSet<String> csvData = env.readTextFile(csvFilePath);

        // 如果有进度跟踪服务，先计算总记录数
        if (progressTrackingService != null && jobId != null) {
            try {
                long totalRecords = csvData.count();
                progressTrackingService.updateTotalRecords(jobId, totalRecords - 1); // 减1是因为要跳过标题行
            } catch (Exception e) {
                System.err.println("计算总记录数失败: " + e.getMessage());
            }
        }

        // 跳过标题行并解析数据
        DataSet<Tuple4<Long, Long, Double, Long>> ratings = csvData
            .filter(line -> !line.startsWith("userId")) // 跳过标题行
            .map(new ParseRatingFunction(progressInterval));

        // 转换为(movieId, rating, 1)格式用于聚合
        DataSet<Tuple3<Long, Double, Long>> ratingData = ratings
                .map(new MapFunction<Tuple4<Long, Long, Double, Long>, Tuple3<Long, Double, Long>>() {
                    @Override
                    public Tuple3<Long, Double, Long> map(Tuple4<Long, Long, Double, Long> value) throws Exception {
                        return new Tuple3<>(value.f1, value.f2, 1L); // movieId, rating, count=1
                    }
                });

        // 按movieId分组并计算平均评分
        DataSet<Tuple3<Long, Double, Long>> movieRatings = ratingData
                .groupBy(0) // 按movieId分组
                .aggregate(Aggregations.SUM, 1) // 对rating求和
                .aggregate(Aggregations.SUM, 2)
                .map(new CalculateAverageFunction());

        // 输出到HBase
        movieRatings.output(new HBaseSinkFunction(hbaseZookeeperQuorum, hbaseZookeeperPort, tableName, columnFamily));

        System.out.println("开始执行Flink作业...");
        env.execute("Movie Rating Calculation Job");
        System.out.println("作业执行完成！");
    }

    // 解析CSV行的函数
    public static class ParseRatingFunction implements MapFunction<String, Tuple4<Long, Long, Double, Long>> {
        private AtomicLong processedCount = new AtomicLong(0);
        private final int progressInterval;

        public ParseRatingFunction(int progressInterval) {
            this.progressInterval = progressInterval;
        }

        @Override
        public Tuple4<Long, Long, Double, Long> map(String line) {
            String[] fields = line.split(",");
            if (fields.length != 4) {
                throw new IllegalArgumentException("Invalid CSV line: " + line);
            }

            long count = processedCount.incrementAndGet();
            if (count % progressInterval == 0) {
                System.out.println("已处理 " + count + " 条数据记录");
            }

            return new Tuple4<>(
                    Long.parseLong(fields[0]), // userId
                    Long.parseLong(fields[1]), // movieId
                    Double.parseDouble(fields[2]), // rating
                    Long.parseLong(fields[3])  // timestamp
            );
        }
    }

    // 计算平均评分的函数
    public static class CalculateAverageFunction implements MapFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>> {
        @Override
        public Tuple3<Long, Double, Long> map(Tuple3<Long, Double, Long> value) throws Exception {
            long movieId = value.f0;
            double totalRating = value.f1;
            long count = value.f2;

            double avgRating = count > 0 ? totalRating / count : 0.0;
            return new Tuple3<>(movieId, avgRating, count);
        }
    }

    // HBase输出函数
    public static class HBaseSinkFunction implements OutputFormat<Tuple3<Long, Double, Long>> {
        private Connection connection;
        private Table table;
        private AtomicLong savedCount = new AtomicLong(0);

        private final String hbaseZookeeperQuorum;
        private final String hbaseZookeeperPort;
        private final String tableName;
        private final String columnFamily;

        public HBaseSinkFunction(String hbaseZookeeperQuorum, String hbaseZookeeperPort, String tableName, String columnFamily) {
            this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
            this.hbaseZookeeperPort = hbaseZookeeperPort;
            this.tableName = tableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void configure(Configuration parameters) {
            // 配置方法，可以为空
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                // 配置HBase连接
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
                config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperPort);

                connection = ConnectionFactory.createConnection(config);

                // 创建表（如果不存在）
                Admin admin = connection.getAdmin();
                TableName hbaseTableName = TableName.valueOf(tableName);

                if (!admin.tableExists(hbaseTableName)) {
                    try {
                        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(hbaseTableName);
                        ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                        tableBuilder.setColumnFamily(cfBuilder.build());
                        admin.createTable(tableBuilder.build());
                        System.out.println("创建HBase表: " + tableName);
                    } catch (org.apache.hadoop.hbase.TableExistsException e) {
                        // 表已存在，忽略此异常（可能是并发创建导致的）
                        System.out.println("HBase表已存在: " + tableName);
                    }
                }

                table = connection.getTable(hbaseTableName);
                admin.close();
            } catch (Exception e) {
                throw new IOException("Failed to initialize HBase connection", e);
            }
        }

        @Override
        public void writeRecord(Tuple3<Long, Double, Long> value) throws IOException {
            try {
                Put put = new Put(Bytes.toBytes(value.f0.toString())); // movieId作为rowkey
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("avg_rating"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("rating_count"), Bytes.toBytes(value.f2.toString()));

                table.put(put);

                long count = savedCount.incrementAndGet();
                if (count % 1000 == 0) {
                    System.out.println("已保存 " + count + " 部电影的评分数据到HBase");
                }
            } catch (Exception e) {
                throw new IOException("Failed to write record to HBase", e);
            }
        }

        @Override
        public void close() throws IOException {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}