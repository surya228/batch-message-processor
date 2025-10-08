package com.oracle.ofss.sanctions.tf.app;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class RawMessageGeneratorMain {
    private static final Logger logger = LoggerFactory.getLogger(RawMessageGeneratorMain.class);

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("=============================================================");
        logger.info("               RAW MESSAGE GENERATOR STARTED                 ");
        logger.info("=============================================================");

        Properties commonProps = loadCommonProperties();
        List<String> enabledConfigs = getEnabledConfigs(commonProps);

        if (enabledConfigs.isEmpty()) {
            logger.error("No enabled configs found in common.properties. Exiting.");
            System.exit(1);
        }

        int totalGeneratedCount = 0;
        JSONArray runDetails = new JSONArray();
        List<String> allFileEntries = new ArrayList<>();
        AtomicInteger currentIndex = new AtomicInteger(1);

        for (String configName : enabledConfigs) {
            logger.info("Processing config: {}", configName);

            // Check if config files exist
            File configFile = new File(Constants.BIN_DIR_PATH, configName + ".properties");
            File sourceFile = new File(Constants.BIN_DIR_PATH, configName + " source.json");

            if (!configFile.exists()) {
                logger.warn("Config file not found: {}. Skipping.", configFile.getPath());
                continue;
            }
            if (!sourceFile.exists()) {
                logger.warn("Source file not found: {}. Skipping.", sourceFile.getPath());
                continue;
            }

            // Load and merge properties
            Properties specificProps = loadPropertiesFromFile(configFile);
            Properties mergedProps = mergeProperties(commonProps, specificProps);

            // Validate required properties
            if (!validateConfigProperties(mergedProps)) {
                logger.error("Validation failed for config: {}. Skipping.", configName);
                continue;
            }

            // Save merged config properties
//            saveConfigProperties(mergedProps, configName);

            // Generate raw messages
            List<SourceInputModel> rawMessages = RawMessageGenerator.generateRawMessage(null, mergedProps, sourceFile.getPath(), configName);
            int generatedCount = rawMessages.size();

            // Calculate file count based on row limit
            int rowLimit = Constants.DEFAULT_ROW_LIMIT;
            try {
                String rowLimitStr = mergedProps.getProperty(Constants.JSON_OBJJECT_LIMIT, String.valueOf(Constants.DEFAULT_ROW_LIMIT));
                rowLimit = Integer.parseInt(rowLimitStr);
            } catch (NumberFormatException e) {
                logger.error("Invalid row limit value for config {}, using default: {}", configName, Constants.DEFAULT_ROW_LIMIT);
            }
            int fileCount = 0;

            if (!rawMessages.isEmpty()) {
                List<String> fileEntries = RawMessageGenerator.writeRawMessagesToJsonFile(rawMessages, mergedProps, configName, currentIndex);
                allFileEntries.addAll(fileEntries);
                fileCount = fileEntries.size();
            }

            // Create run details object for this config
            JSONObject configDetails = new JSONObject();
            configDetails.put("configName", configName);
            configDetails.put("fileCount", fileCount);
            configDetails.put("rawMessageCount", generatedCount);
            runDetails.put(configDetails);

            if (generatedCount > 0) {
                logger.info("Config {}: Generated {} raw messages across {} JSON files.", configName, generatedCount, fileCount);
                totalGeneratedCount += generatedCount;
            } else {
                logger.info("Config {}: No raw messages generated.", configName);
            }
        }

        if (totalGeneratedCount == 0) {
            logger.info("No raw messages generated for any config. Exiting utility.");
            System.exit(0);
        }

        // Write filename.txt
        if (!allFileEntries.isEmpty()) {
            File listFile = new File(Constants.OUTPUT_FOLDER, Constants.FILE_NAME_LIST);
            try (BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(listFile))) {
                for (String fileEntry : allFileEntries) {
                    writer.write(fileEntry);
                    writer.newLine();
                }
            } catch (IOException e) {
                logger.error("Error writing filename.txt: {}", e.getMessage());
            }
            logger.info("filename.txt created with list of files.");
        }

        // Write run details to JSON file
        writeRunDetailsToFile(runDetails);

        logger.info("=============================================================");
        logger.info("               RAW MESSAGE GENERATOR ENDED                 ");
        logger.info("=============================================================");
        logger.info("Total messages generated across all configs: {}", totalGeneratedCount);
        logger.info("Total time taken by utility: {} Seconds ", (System.currentTimeMillis() - startTime) / 1000L );
    }

    /**
     * Saves the configuration properties to a file with the config name.
     * @param props Properties to save.
     * @param configName The config name.
     */
    private static void saveConfigProperties(Properties props, String configName) {
        String fileName = configName + ".properties";
        File configFile = new File(Constants.OUTPUT_FOLDER, fileName);
        File commonConfig = new File(Constants.COMMON_CONFIG_FILE_PATH);
        try {
            Files.copy(commonConfig.toPath(), configFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            logger.info("Config properties saved to {}", configFile.getName());
        } catch (IOException e) {
            logger.error("Error saving config properties: {}", e.getMessage());
        }
    }
    private static Properties loadCommonProperties() throws IOException {
        Properties props = new Properties();
        File commonConfigFile = new File(Constants.COMMON_CONFIG_FILE_PATH);
        try (FileReader reader = new FileReader(commonConfigFile)) {
            props.load(reader);
            logger.info("Common properties file loaded from: {}", commonConfigFile.getPath());
        } catch (IOException e) {
            logger.error("Error reading common properties file: {}", e.getMessage());
            throw e;
        }
        return props;
    }

    private static List<String> getEnabledConfigs(Properties commonProps) {
        List<String> enabledConfigs = new ArrayList<>();
        for (String key : commonProps.stringPropertyNames()) {
            if (key.endsWith(".enabled") && Constants.YES.equalsIgnoreCase(commonProps.getProperty(key))) {
                String configName = key.substring(0, key.length() - ".enabled".length());
                enabledConfigs.add(configName);
            }
        }
        logger.info("Found {} enabled configs: {}", enabledConfigs.size(), enabledConfigs);
        return enabledConfigs;
    }

    private static Properties loadPropertiesFromFile(File file) throws IOException {
        Properties props = new Properties();
        try (FileReader reader = new FileReader(file)) {
            props.load(reader);
            logger.info("Properties file loaded from: {}", file.getPath());
        } catch (IOException e) {
            logger.error("Error reading properties file {}: {}", file.getPath(), e.getMessage());
            throw e;
        }
        return props;
    }

    private static Properties mergeProperties(Properties commonProps, Properties specificProps) {
        Properties merged = new Properties();
        merged.putAll(commonProps);
        merged.putAll(specificProps); // Specific overrides common
        logger.info("Properties merged: common keys={}, specific keys={}, merged keys={}",
                   commonProps.size(), specificProps.size(), merged.size());
        return merged;
    }

    private static boolean validateConfigProperties(Properties props) {
        String batchType = props.getProperty(Constants.GENERATOR_BATCH_TYPE);
        if (batchType == null || (!batchType.equalsIgnoreCase("ISO20022") && !batchType.equalsIgnoreCase("NACHA"))) {
            logger.error("Invalid or missing batchtype. Must be 'ISO20022' or 'NACHA'.");
            return false;
        }
        String misDate = props.getProperty(Constants.MIS_DATE);
        if (misDate == null || misDate.isEmpty()) {
            logger.error("Missing misdate.");
            return false;
        }
        String runNo = props.getProperty(Constants.RUN_NO);
        if (runNo == null || runNo.isEmpty()) {
            logger.error("Missing runNo.");
            return false;
        }
        return true;
    }

    /**
     * Writes the run details JSONArray to run_details.json file.
     * @param runDetails The JSONArray containing run details for all configs
     */
    private static void writeRunDetailsToFile(JSONArray runDetails) {
        if (!Constants.OUTPUT_FOLDER.exists()) {
            Constants.OUTPUT_FOLDER.mkdirs();
        }

        File runDetailsFile = new File(Constants.OUTPUT_FOLDER, Constants.RUN_DETAILS_FILE_NAME);
        try (java.io.FileWriter writer = new java.io.FileWriter(runDetailsFile)) {
            writer.write(runDetails.toString(2)); // Pretty print with 2-space indentation
            logger.info("Run details written to: {}", runDetailsFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Error writing run details to file: {}", e.getMessage());
        }
    }
}
