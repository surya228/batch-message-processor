package com.oracle.ofss.sanctions.tf.app;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class AnalyzerMain {
    private static Logger logger = LoggerFactory.getLogger(AnalyzerMain.class);
    public static void main(String[] args) throws Exception {
        logger.info("Hello World from Analyzer Main!!!");


        Properties props = loadProperties();
        String watchListType = props.getProperty(Constants.WATCHLIST_TYPE);
        String webServiceId = props.getProperty(Constants.WEBSERVICE_ID);
        String tagName = props.getProperty(Constants.TAGNAME);
        String runSkey = props.getProperty(Constants.RUN_SKEY);
        String batchType = props.getProperty(Constants.BATCH_TYPE);

        int msgCategory = batchType.equalsIgnoreCase("ISO20022")?3:4;
        String msgCategoryString = batchType.equalsIgnoreCase("ISO20022")?"SEPA":"NACHA";


        // Parallel processing of each trxn token
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        List<Long> transactionTokens;
        Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap;
        Map<Long, JSONObject> feedbackMap;
        Map<Long, JSONObject> tokenToAdditionalDataMap;
        try (Connection connection = SQLUtility.getDbConnection()) {
            transactionTokens = getLisOfAllTransactions(connection,runSkey);
            tokenToResponseIdToColumnNamesMap = getBulkColumnNameWLS(connection,transactionTokens,msgCategory);
            feedbackMap = getBulkResponsesFromFeedbackTable(connection,transactionTokens, msgCategoryString);
            tokenToAdditionalDataMap = getTokenToAdditionalDataMap(connection,transactionTokens,msgCategory);
        } catch (Exception e) {
            logger.error("Error during database operations: {}", e.getMessage(), e);
            throw e;
        }


        for (long transactionToken : transactionTokens) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    JSONObject eachResponse = feedbackMap.get(transactionToken);
                    if (eachResponse == null || !eachResponse.has(Constants.MATCHES)) return;

                    JSONArray matches = eachResponse.getJSONArray(Constants.MATCHES);
                    int truePositives = 0;
                    String targetColumnName;
                    String uid="";
                    boolean valueNotPresentFlag = false;
                    JSONObject additionalData = tokenToAdditionalDataMap.get(transactionToken);
                    if(additionalData.has(Constants.UID) && additionalData.has(Constants.COLUMN)){
                        uid = additionalData.getString(Constants.UID);
                        targetColumnName = additionalData.getString(Constants.COLUMN);
                    } else{
                        targetColumnName = "";
                        logger.error("trxn token: {} doesn't have either uid or column data. Can't do comparison for this token",transactionToken);
                        valueNotPresentFlag = true;
                    }

                    Map<Long, String> responseIdColumnNamesMap = tokenToResponseIdToColumnNamesMap.getOrDefault(transactionToken, Collections.emptyMap());

                    boolean failedDueToColumnMismatch = false;
                    if(!valueNotPresentFlag) {
                        for (int i = 0; i < matches.length(); i++) {
                            JSONObject match = matches.getJSONObject(i);
                            String tagNameCsv = match.optString("tagName", "");
                            Set<String> tagNames = Arrays.stream(tagNameCsv.split(",")).map(String::trim).collect(Collectors.toSet());

                            String targetUid = match.getString(Constants.MATCHED_WATCHLIST_ID);
                            Long responseId = match.getLong(Constants.RESPONSE_ID);
                            String columnNameWls = responseIdColumnNamesMap.get(responseId);
                            Set<String> columnNames = columnNameWls != null ? Arrays.stream(columnNameWls.split(",")).collect(Collectors.toSet()) : Collections.emptySet();

                            boolean flag = uid.equals(targetUid)
                                    && watchListType.equalsIgnoreCase(match.optString("watchlistType"))
                                    && webServiceId.equalsIgnoreCase(String.valueOf(match.getInt("webServiceID")))
                                    && tagNames.contains(tagName);

                            if (flag) {
                                if (columnNames.stream().anyMatch(col -> col.equalsIgnoreCase(targetColumnName))) { // Case-insensitive match
                                    truePositives++;
                                    failedDueToColumnMismatch = false;
                                    break; // Early exit if we only need count >=1
                                } else {
                                    failedDueToColumnMismatch = true;
                                }
                            }
                        }
                    }
                    String testStatus = truePositives > 0 ? Constants.PASS : Constants.FAIL;
                    logger.info("Status for trxn toke {} : {}",transactionToken,testStatus);
                    if (Constants.FAIL.equalsIgnoreCase(testStatus))
                        logger.info("failedDueToColumnMismatch : {}",failedDueToColumnMismatch);
                } catch (Exception e) {
                    logger.error("Error processing transactionToken {}: {}", transactionToken, e.getMessage(), e);
                }
            }, executor);
            futures.add(future);
        }

        // Wait for all tasks to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            logger.error("Error during parallel processing: {}", e.getMessage(), e);
        } finally {
            executor.shutdown();
        }

    }

    private static Map<Long, JSONObject> getTokenToAdditionalDataMap(Connection connection, List<Long> transactionTokens, int msgCategory) throws Exception {
        Map<Long, JSONObject> tokenToAdditionalDetails = new HashMap<>();
        if (transactionTokens.isEmpty()) return tokenToAdditionalDetails;

        String tableName = msgCategory==3?"fcc_tf_xml_raw_data":"fcc_tf_ach_raw_data";
        // Batch in chunks to avoid IN clause limits
        int batchSize = 1000;
        for (int i = 0; i < transactionTokens.size(); i += batchSize) {
            List<Long> batch = transactionTokens.subList(i, Math.min(i + batchSize, transactionTokens.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String query = "select N_GRP_MSG_ID, C_ADDITIONAL_DATA from "+tableName+" where n_grp_msg_id in (" + placeholders + ")";
            try (PreparedStatement pst = connection.prepareStatement(query)) {
                for (int j = 0; j < batch.size(); j++) {
                    pst.setLong(j + 1, batch.get(j));
                }
                try (ResultSet rs = pst.executeQuery()) {
                    while (rs.next()) {
                        String jsonString = rs.getString("C_ADDITIONAL_DATA");
                        if (jsonString != null && !jsonString.isEmpty()) {
                            tokenToAdditionalDetails.put(rs.getLong("N_GRP_MSG_ID"), new JSONObject(jsonString));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error in getTokenToAdditionalDataMap for batch starting at index {}: {}", i, e.getMessage(), e);
                throw e;
            }
        }
        return tokenToAdditionalDetails;
    }

    private static List<Long> getLisOfAllTransactions(Connection connection, String runSkey) throws Exception {
        List<Long> tokens = new ArrayList<>();
        String query = "SELECT N_GRP_MSG_ID FROM FCC_TF_XML_BATCH_TRXN WHERE N_RUN_SKEY = ?";
        try (PreparedStatement pst = connection.prepareStatement(query)) {
            pst.setLong(1, Long.parseLong(runSkey));
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    tokens.add(rs.getLong("N_GRP_MSG_ID"));
                }
            }
        } catch (Exception e) {
            logger.error("Error in getLisOfAllTransactions: {}", e.getMessage(), e);
            throw e;
        }
        return tokens;
    }

    private static Map<Long, JSONObject> getBulkResponsesFromFeedbackTable(Connection connection, List<Long> transactionTokens, String msgCategory) throws Exception {
        Map<Long, JSONObject> feedbackMap = new HashMap<>();
        if (transactionTokens.isEmpty()) return feedbackMap;
        // Batch in chunks to avoid IN clause limits
        int batchSize = 1000;
        for (int i = 0; i < transactionTokens.size(); i += batchSize) {
            List<Long> batch = transactionTokens.subList(i, Math.min(i + batchSize, transactionTokens.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String query = "SELECT N_TRAX_TOKEN, C_FEEDBACK_MESSAGE FROM fcc_tf_feedback WHERE N_TRAX_TOKEN IN (" + placeholders + ") AND V_MSG_CATEGORY = ?";
            try (PreparedStatement pst = connection.prepareStatement(query)) {
                for (int j = 0; j < batch.size(); j++) {
                    pst.setLong(j + 1, batch.get(j));
                }
                pst.setString(batch.size() + 1, msgCategory);
                try (ResultSet rs = pst.executeQuery()) {
                    while (rs.next()) {
                        String jsonString = rs.getString("C_FEEDBACK_MESSAGE");
                        if (jsonString != null && !jsonString.isEmpty()) {
                            feedbackMap.put(rs.getLong("N_TRAX_TOKEN"), new JSONObject(jsonString));
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error in getBulkResponsesFromFeedbackTable for batch starting at index {}: {}", i, e.getMessage(), e);
                throw e;
            }
        }
        return feedbackMap;
    }

    private static Map<Long, Map<Long, String>> getBulkColumnNameWLS(Connection connection, List<Long> transactionTokens, int msgCategory) throws Exception {
        Map<Long, Map<Long, String>> tokenToColumnMap = new HashMap<>();
        if (transactionTokens.isEmpty()) return tokenToColumnMap;
        // Batch in chunks to avoid IN clause limits
        int batchSize = 1000;
        for (int i = 0; i < transactionTokens.size(); i += batchSize) {
            List<Long> batch = transactionTokens.subList(i, Math.min(i + batchSize, transactionTokens.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String query = "SELECT N_GRP_MSG_ID, N_RESPONSE_ID, V_COLUMN_NAME FROM fcc_tf_rt_wls_response WHERE n_grp_msg_id IN (" + placeholders + ") AND n_msg_category = ?";
            try (PreparedStatement pst = connection.prepareStatement(query)) {
                for (int j = 0; j < batch.size(); j++) {
                    pst.setLong(j + 1, batch.get(j));
                }
                pst.setInt(batch.size() + 1, msgCategory);
                try (ResultSet rs = pst.executeQuery()) {
                    while (rs.next()) {
                        long token = rs.getLong("N_GRP_MSG_ID");
                        long responseId = rs.getLong("N_RESPONSE_ID");
                        String columnName = rs.getString("V_COLUMN_NAME");
                        tokenToColumnMap.computeIfAbsent(token, k -> new HashMap<>()).put(responseId, columnName);
                    }
                }
            } catch (Exception e) {
                logger.error("Error in getBulkColumnNameWLS for batch starting at index {}: {}", i, e.getMessage(), e);
                throw e;
            }
        }
        return tokenToColumnMap;
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
