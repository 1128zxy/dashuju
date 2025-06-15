package com.abc.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.io.IOException;

@Configuration
public class HBaseConfig {
    
    @Value("${hbase.zookeeper.quorum:localhost}")
    private String zookeeperQuorum;
    
    @Value("${hbase.zookeeper.port:2181}")
    private String zookeeperPort;
    
    private Connection connection;
    
    @Bean
    public org.apache.hadoop.conf.Configuration hbaseConfiguration() {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        
        // 设置HBase客户端配置
        config.set("hbase.client.write.buffer", "2097152");
        config.set("hbase.client.pause", "200");
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.rpc.timeout", "60000");
        config.set("hbase.client.operation.timeout", "60000");
        config.set("hbase.client.scanner.timeout.period", "60000");
        
        return config;
    }
    
    @Bean
    public Connection hbaseConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(hbaseConfiguration());
        }
        return connection;
    }
    
    @PreDestroy
    public void closeConnection() {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                System.err.println("关闭HBase连接时发生错误: " + e.getMessage());
            }
        }
    }
}