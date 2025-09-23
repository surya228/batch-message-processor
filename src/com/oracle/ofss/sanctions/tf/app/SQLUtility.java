package com.oracle.ofss.sanctions.tf.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class SQLUtility {
    private static final Logger logger = LoggerFactory.getLogger(SQLUtility.class);
    private static HikariDataSource dataSource;

    static {
        try {
            Properties props = new Properties();
            try (FileReader reader = new FileReader(Constants.COMMON_CONFIG_FILE_PATH)) {
                props.load(reader);
            }

            String jdbcUrl = props.getProperty(Constants.JDBC_URL);
            String jdbcDriver = props.getProperty(Constants.JDBC_DRIVER);
            String walletname = props.getProperty(Constants.WALLET_NAME);
            String tnsAdminPath = Constants.PARENT_DIRECTORY + File.separator + Constants.BIN_FOLDER_NAME + File.separator + walletname;

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setDriverClassName(jdbcDriver);
            config.addDataSourceProperty(Constants.TNS_ADMIN, tnsAdminPath);

            // Optimized connection pool settings for bulk operations
            config.setMaximumPoolSize(20); // Increased for better throughput
            config.setMinimumIdle(10);
            config.setConnectionTimeout(60000); // 60 seconds
            config.setIdleTimeout(300000); // 5 minutes
            config.setMaxLifetime(1800000); // 30 minutes
            config.setLeakDetectionThreshold(60000); // 60 seconds

            // Performance optimizations
            config.addDataSourceProperty("oracle.jdbc.ReadTimeout", "60000");
            config.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", "10000");
            config.addDataSourceProperty("oracle.jdbc.defaultNChar", "true");

            dataSource = new HikariDataSource(config);
        } catch (Exception e) {
            logger.error("Error initializing connection pool: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static Connection getDbConnection() throws Exception {
        Connection connection = dataSource.getConnection();
        logger.info(Constants.CONNECTION_ESTABLISHED);
        return connection;
    }
}
