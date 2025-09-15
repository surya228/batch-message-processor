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
        logger.info("      INTELLIGENT RAW MESSAGE PROCESSOR UTILITY STARTED      ");
        logger.info("=============================================================");
        Date startDateObj = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DATE_SUFFIX_FORMAT);
        SimpleDateFormat timeFormat = new SimpleDateFormat(Constants.TIME_SUFFIX_FORMAT);
        String startDate = dateFormat.format(startDateObj);
        String startTimeStr = timeFormat.format(startDateObj);

        Properties props = loadProperties();
        saveConfigProperties(props);

        String renamePrefix = props.getProperty(Constants.WEBSERVICE) + "_";

        long startTime = System.currentTimeMillis();

        boolean isToggle = Constants.YES.equalsIgnoreCase(props.getProperty(Constants.TOGGLE_MATCHING_ENGINE));

        // Delete previous output files and count file
        deletePreviousFiles();

        // grenerate raw message
        int generatedCount = RawMessageGenerator.generateRawMessage(null, props); // Generation is always sequential
        if (generatedCount == 0) {
            logger.info("No raw messages generated. Exiting utility.");
            System.exit(0);
        }
        logger.info("Raw Message Generator Completed");

        if (generatedCount > 0) {
            int fileCount = 1;
            try {
                File countFile = new File(Constants.OUTPUT_FOLDER, Constants.OUTPUT_FILE_COUNT_PATH);
                if (countFile.exists()) {
                    String countStr = new String(Files.readAllBytes(countFile.toPath())).trim();
                    fileCount = Integer.parseInt(countStr);
                }
            } catch (Exception e) {
                logger.error("Error reading file count: {}", e.getMessage());
            }
            logger.info("Generated {} raw messages across {} Excel files.", generatedCount, fileCount);
        }

        logger.info("=============================================================");
        logger.info("     INTELLIGENT RAW MESSAGE PROCESSOR UTILITY COMPLETED     ");
        logger.info("=============================================================");
        long endTime = System.currentTimeMillis();
        logger.info("Total time taken by utility: {} Seconds ", (endTime - startTime) / 1000L );
    }

    private static void deletePreviousFiles() {
        File countFile = new File(Constants.OUTPUT_FOLDER, Constants.OUTPUT_FILE_COUNT_PATH);
        if (countFile.exists()) {
            if (countFile.delete()) {
                logger.info("Deleted previous count file: {}", countFile.getName());
            } else {
                logger.error("Failed to delete previous count file: {}", countFile.getName());
            }
        }
        File[] prevFiles = Constants.OUTPUT_FOLDER.listFiles((dir, name) -> name.matches(Constants.OUTPUT_FILE_NAME+"_\\d+\\.xlsx"));
        if (prevFiles != null) {
            for (File file : prevFiles) {
                if (file.delete()) {
                    logger.info("Deleted previous output file: {}", file.getName());
                } else {
                    logger.error("Failed to delete previous output file: {}", file.getName());
                }
            }
        }
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
