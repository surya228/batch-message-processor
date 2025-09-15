package com.oracle.ofss.sanctions.tf.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class RawMessageGeneratorMain {
    private static final Logger logger = LoggerFactory.getLogger(RawMessageGeneratorMain.class);

    public static void main(String[] args) throws Exception {
        logger.info("=============================================================");
        logger.info("               RAW MESSAGE GENERATOR STARTED                 ");
        logger.info("=============================================================");
        Date startDateObj = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DATE_SUFFIX_FORMAT);
        SimpleDateFormat timeFormat = new SimpleDateFormat(Constants.TIME_SUFFIX_FORMAT);
        String startDate = dateFormat.format(startDateObj);
        String startTimeStr = timeFormat.format(startDateObj);

        Properties props = loadProperties();

        // Validate new properties
        String batchType = props.getProperty(Constants.BATCH_TYPE);
        if (batchType == null || (!batchType.equalsIgnoreCase("ISO20022") && !batchType.equalsIgnoreCase("NACHA"))) {
            logger.error("Invalid or missing batchtype in config.properties. Must be 'ISO20022' or 'NACHA'.");
            System.exit(1);
        }
        String misDate = props.getProperty(Constants.MIS_DATE);
        if (misDate == null || misDate.isEmpty()) {
            logger.error("Missing misdate in config.properties.");
            System.exit(1);
        }
        String runNo = props.getProperty(Constants.RUN_NO);
        if (runNo == null || runNo.isEmpty()) {
            logger.error("Missing runNo in config.properties.");
            System.exit(1);
        }

        saveConfigProperties(props);

        String renamePrefix = props.getProperty(Constants.WEBSERVICE) + "_";

        long startTime = System.currentTimeMillis();

        boolean isToggle = Constants.YES.equalsIgnoreCase(props.getProperty(Constants.TOGGLE_MATCHING_ENGINE));

        // grenerate raw message
        int generatedCount = RawMessageGenerator.generateRawMessage(null, props); // Generation is always sequential
        if (generatedCount == 0) {
            logger.info("No raw messages generated. Exiting utility.");
            System.exit(0);
        }
        logger.info("Raw Message Generator Completed");

        if (generatedCount > 0) {
            // Calculate file count based on row limit
            int rowLimit = Constants.DEFAULT_ROW_LIMIT;
            try {
                String rowLimitStr = props.getProperty(Constants.EXCEL_SPLIT_ROW_LIMIT, String.valueOf(Constants.DEFAULT_ROW_LIMIT));
                rowLimit = Integer.parseInt(rowLimitStr);
            } catch (NumberFormatException e) {
                logger.error("Invalid row limit value, using default: {}", Constants.DEFAULT_ROW_LIMIT);
            }
            int fileCount = (int) Math.ceil((double) generatedCount / rowLimit);
            logger.info("Generated {} raw messages across {} JSON files.", generatedCount, fileCount);
        }

        logger.info("=============================================================");
        logger.info("               RAW MESSAGE GENERATOR ENDED                 ");
        logger.info("=============================================================");
        long endTime = System.currentTimeMillis();
        logger.info("Total time taken by utility: {} Seconds ", (endTime - startTime) / 1000L );
    }

    /**
     * Saves the configuration properties to a file with a name based on the webservice.
     * @param props Properties to save.
     */
    private static void saveConfigProperties(Properties props) {
        String webservice = props.getProperty(Constants.WEBSERVICE);
        String fileName = (webservice != null ? webservice : Constants.DEFAULT_CONFIG_BASE) + ".properties";
        File configFile = new File(Constants.OUTPUT_FOLDER, fileName);
        File originalConfig = new File(Constants.CONFIG_FILE_PATH);
        try {
            Files.copy(originalConfig.toPath(), configFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            logger.info("Config properties saved to {}", configFile.getName());
        } catch (IOException e) {
            logger.error("Error saving config properties: {}", e.getMessage());
        }
    }

    private static Properties loadProperties() throws IOException {
        Properties props = new Properties();
        try (FileReader reader = new FileReader(Constants.CONFIG_FILE_PATH)) {
            props.load(reader);
            logger.info("Properties file loaded");
        } catch (IOException e) {
            logger.error("Error reading properties file: {}", e.getMessage());
            throw e;
        }
        return props;
    }
}
