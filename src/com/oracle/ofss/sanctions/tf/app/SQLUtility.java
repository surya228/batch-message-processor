package com.oracle.ofss.sanctions.tf.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class SQLUtility {
    private static final Logger logger = LoggerFactory.getLogger(SQLUtility.class);
    public static Connection getDbConnection() throws Exception {
        Properties props = new Properties();
        try (FileReader reader = new FileReader(Constants.CONFIG_FILE_PATH)) {
            props.load(reader);
        } catch (IOException e) {
            logger.error("Error reading properties file: {}", e.getMessage());
            throw e;
        }

        String jdbcUrl = props.getProperty(Constants.JDBC_URL);
        String jdbcDriver = props.getProperty(Constants.JDBC_DRIVER);
        String walletname = props.getProperty(Constants.WALLET_NAME);
        String tnsAdminPath = Constants.PARENT_DIRECTORY+ File.separator+Constants.BIN_FOLDER_NAME+File.separator+walletname;

        Properties properties = new Properties();
        properties.setProperty("oracle.net.tns_admin", tnsAdminPath);
        Class.forName(jdbcDriver);
        Connection connection = DriverManager.getConnection(jdbcUrl,properties);
        logger.info("Connection established successfully!");
        return connection;
    }
}
