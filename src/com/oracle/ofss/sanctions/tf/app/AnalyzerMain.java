package com.oracle.ofss.sanctions.tf.app;

import org.apache.poi.ss.usermodel.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class AnalyzerMain {
    private static Logger logger = LoggerFactory.getLogger(AnalyzerMain.class);
    public static void main(String[] args) throws Exception {
        logger.info("Hello World from Analyzer Main!!!");

        Properties props = loadProperties();
        String watchListType = props.getProperty(Constants.WATCHLIST_TYPE);
        String webServiceId = props.getProperty(Constants.WEBSERVICE_ID);
        String webService = props.getProperty(Constants.WEBSERVICE);
        String tagName = props.getProperty(Constants.TAGNAME);
        String runSkey = props.getProperty(Constants.RUN_SKEY);
        String batchType = props.getProperty(Constants.BATCH_TYPE);

        int msgCategory = batchType.equalsIgnoreCase("ISO20022")?3:4;
        String msgCategoryString = batchType.equalsIgnoreCase("ISO20022")?"SEPA":"NACHA";

        String misDate = props.getProperty(Constants.MIS_DATE);
        String runNo = props.getProperty(Constants.RUN_NO);

        String matchHeader = "OS # " + Constants.getMatchHeaderSuffix(webServiceId, watchListType) + " matches";

        Map<Long, String> tokenToRawMsg;
        List<Long> transactionTokens;
        Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap;
        Map<Long, JSONObject> feedbackMap;
        Map<Long, JSONObject> tokenToAdditionalDataMap;
        try (Connection connection = SQLUtility.getDbConnection()) {
            tokenToRawMsg = getTokenToRawMsg(connection, runSkey, batchType);
            transactionTokens = new ArrayList<>(tokenToRawMsg.keySet());
            feedbackMap = getBulkResponsesFromFeedbackTable(connection,transactionTokens, msgCategoryString);
            tokenToResponseIdToColumnNamesMap = getBulkColumnNameWLS(connection,transactionTokens,msgCategory);
            tokenToAdditionalDataMap = getTokenToAdditionalDataMap(connection,transactionTokens,msgCategory);
        } catch (Exception e) {
            logger.error("Error during database operations: {}", e.getMessage(), e);
            throw e;
        }

        List<ReportRow> reportRows = analyzeResults(transactionTokens, tokenToResponseIdToColumnNamesMap, feedbackMap, tokenToAdditionalDataMap, watchListType, webServiceId, webService, tagName, runSkey, tokenToRawMsg);

        writeExcel(reportRows, misDate, runNo, batchType, matchHeader);

    }

    private static List<ReportRow> analyzeResults(List<Long> transactionTokens, Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap,
                                       Map<Long, JSONObject> feedbackMap, Map<Long, JSONObject> tokenToAdditionalDataMap,
                                       String watchListType, String webServiceId, String webService, String tagName, String runSkey, Map<Long, String> tokenToRawMsg) {
        // Parallel processing of each trxn token
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ConcurrentLinkedQueue<ReportRow> queue = new ConcurrentLinkedQueue<>();
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
                    int filteredCount = 0;
                    if(!valueNotPresentFlag) {
                        for (int i = 0; i < matches.length(); i++) {
                            JSONObject match = matches.getJSONObject(i);
                            String tagNameCsv = match.optString("tagName", "");
                            Set<String> tagNames = Arrays.stream(tagNameCsv.split(",")).map(String::trim).collect(Collectors.toSet());

                            String targetUid = match.getString(Constants.MATCHED_WATCHLIST_ID);
                            Long responseId = match.getLong(Constants.RESPONSE_ID);
                            String columnNameWls = responseIdColumnNamesMap.get(responseId);
                            Set<String> columnNames = columnNameWls != null ? Arrays.stream(columnNameWls.split(",")).collect(Collectors.toSet()) : Collections.emptySet();

                            // Filtered count for OS # ... matches
                            if (String.valueOf(match.optInt("webServiceID")).equals(webServiceId) &&
                                    (!webServiceId.equals("3") && !webServiceId.equals("4") || match.optString("watchlistType").equalsIgnoreCase(watchListType))) {
                                filteredCount++;
                            }

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
                        String testStatus = truePositives > 0 ? Constants.PASS : Constants.FAIL;
                        logger.info("Status for trxn toke {} : {}",transactionToken,testStatus);
                        if (Constants.FAIL.equalsIgnoreCase(testStatus))
                            logger.info("failedDueToColumnMismatch : {}",failedDueToColumnMismatch);

                        // Collect report row data
                        int ced = additionalData.optInt(Constants.CED, 0);
                        String type = "";
                        if (ced == 0) type = Constants.EXACT;
                        else if (ced > 0) type = Constants.FUZZY + ced + Constants.CED;
                        else if (ced == -1) type = "STOPWORD";
                        else if (ced == -2) type = "SYNONYM";

                        String ruleName = webService +" "+type;

                        String message = tokenToRawMsg.getOrDefault(transactionToken, "");
                        String sourceInput = additionalData.optString(Constants.VALUE, "");
                        String targetInput = additionalData.optString(Constants.ORIGINAL_VALUE, "");
                        int matchCount = eachResponse.optInt(Constants.MATCHING_COUNT, 0);
                        String feedbackStatus = eachResponse.optString(Constants.MATCHING_STATUS, "");
                        String feedback = eachResponse.toString();
                        if (feedback.length() > 32767) {
                            feedback = "Value too large check feedback table";
                        }
                        String comments = "";
                        if (Constants.FAIL.equals(testStatus)) {
                            comments = failedDueToColumnMismatch ? Constants.COLUMN_MISMATCH_COMMENT : Constants.NO_MATCH_COMMENT;
                        }

                        ReportRow row = new ReportRow(0, ruleName, message, tagName, sourceInput, targetInput,
                                targetColumnName, watchListType, uid, transactionToken, runSkey,
                                matchCount, feedbackStatus, filteredCount, feedback, testStatus, comments, additionalData.optString("MessageKey", ""));
                        queue.add(row);
                    }

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

        List<ReportRow> reportRows = new ArrayList<>(queue);
        reportRows.sort(Comparator.comparingLong(rr -> rr.osTransactionToken));
        return reportRows;
    }

    private static void writeExcel(List<ReportRow> reportRows, String misDate, String runNo, String batchType, String matchHeader) throws IOException {
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet("Analysis");

            Font boldFont = wb.createFont();
            boldFont.setBold(true);

            CellStyle highlightGreen = wb.createCellStyle();
            highlightGreen.setFillForegroundColor(IndexedColors.BRIGHT_GREEN.getIndex());
            highlightGreen.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            highlightGreen.setFont(boldFont);

            CellStyle highlightRed = wb.createCellStyle();
            highlightRed.setFillForegroundColor(IndexedColors.RED.getIndex());
            highlightRed.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            highlightRed.setFont(boldFont);

            String[] headers = {
                    "SeqNo", "Rule Name", "Message ISO20022", "Tag", "Source Input", "Target Input",
                    "Target Column", "Watchlist", "N_UID", "OS Transaction Token", "OS RunSkey",
                    "OS Match Count", "OS Feedback Status", matchHeader, "OS Feedback",
                    "OS Test Status", "OS Comments", "Message Key"
            };
            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < headers.length; i++) {
                headerRow.createCell(i).setCellValue(headers[i]);
            }

            int rowNum = 1;
            for (ReportRow rr : reportRows) {
                Row row = sheet.createRow(rowNum);
                row.createCell(0).setCellValue(rowNum);
                row.createCell(1).setCellValue(rr.ruleName);
                row.createCell(2).setCellValue(rr.message);
                row.createCell(3).setCellValue(rr.tag);
                row.createCell(4).setCellValue(rr.sourceInput);
                row.createCell(5).setCellValue(rr.targetInput);
                row.createCell(6).setCellValue(rr.targetColumn);
                row.createCell(7).setCellValue(rr.watchlist);
                row.createCell(8).setCellValue(rr.nUid);
                row.createCell(9).setCellValue(rr.osTransactionToken);
                row.createCell(10).setCellValue(rr.osRunSkey);
                row.createCell(11).setCellValue(rr.osMatchCount);
                row.createCell(12).setCellValue(rr.osFeedbackStatus);
                row.createCell(13).setCellValue(rr.osSpecificMatches);
                row.createCell(14).setCellValue(rr.osFeedback);
                row.createCell(15).setCellValue(rr.osTestStatus);
                if (Constants.PASS.equals(rr.osTestStatus)) {
                    row.getCell(15).setCellStyle(highlightGreen);
                } else if (Constants.FAIL.equals(rr.osTestStatus)) {
                    row.getCell(15).setCellStyle(highlightRed);
                }
                row.createCell(16).setCellValue(rr.osComments);
                row.createCell(17).setCellValue(rr.messageKey);
                rowNum++;
            }

            String prefix;
            if ("ISO20022".equalsIgnoreCase(batchType)) {
                prefix = misDate + "_RUN" + runNo + "_STG_ANALYSIS_";
            } else if ("NACHA".equalsIgnoreCase(batchType)) {
                prefix = misDate + "_RUN" + runNo + "_ACH_ANALYSIS_";
            } else {
                throw new IllegalArgumentException("Invalid batchType");
            }
            String fileName = prefix + ".xlsx";
            File outputFile = new File(Constants.OUTPUT_FOLDER, fileName);
            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                wb.write(fos);
            }
            logger.info("Excel report generated at: {}", outputFile.getAbsolutePath());
        }
    }

    private static Map<Long, String> getTokenToRawMsg(Connection connection, String runSkey, String batchType) throws Exception {
        Map<Long, String> map = new HashMap<>();
        String tableName = batchType.equalsIgnoreCase("ISO20022") ? "FCC_TF_XML_BATCH_TRXN" : "FCC_TF_ACH_BATCH_TRXN"; // Assumed ACH table
        String query = "SELECT N_GRP_MSG_ID, C_RAW_MSG FROM " + tableName + " WHERE N_RUN_SKEY = ?";
        try (PreparedStatement pst = connection.prepareStatement(query)) {
            pst.setLong(1, Long.parseLong(runSkey));
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    map.put(rs.getLong("N_GRP_MSG_ID"), rs.getString("C_RAW_MSG"));
                }
            }
        } catch (Exception e) {
            logger.error("Error in getTokenToRawMsg: {}", e.getMessage(), e);
            throw e;
        }
        if (map.isEmpty()) {
            throw new Exception("No data found in "+tableName+" for the runSkey: "+ runSkey);
        }
    return map;
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
                        } else {
                            feedbackMap.put(rs.getLong("N_TRAX_TOKEN"), new JSONObject("No Feedback Found."));
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
