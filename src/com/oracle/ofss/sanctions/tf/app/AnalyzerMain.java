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
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

public class AnalyzerMain {
    private static Logger logger = LoggerFactory.getLogger(AnalyzerMain.class);
    public static void main(String[] args) throws Exception {
        logger.info("Hello World from Analyzer Main!!!");

        Properties props = loadProperties();
        String osRunSkey = props.getProperty(Constants.OS_RUN_SKEY);
        String otRunSkey = props.getProperty(Constants.OT_RUN_SKEY);
        String batchType = props.getProperty(Constants.ANALYZER_BATCH_TYPE);
        int excelRowLimit = Integer.parseInt(props.getProperty(Constants.EXCEL_ROW_LIMIT, String.valueOf(Constants.DEFAULT_EXCEL_ROW_LIMIT)));

        int msgCategory = batchType.equalsIgnoreCase(Constants.ISO20022)?Constants.THREE:Constants.FOUR;
        String msgCategoryString = batchType.equalsIgnoreCase(Constants.ISO20022)?Constants.SEPA:Constants.NACHA;

        String misDate = props.getProperty(Constants.MIS_DATE);
        String runNo = props.getProperty(Constants.RUN_NO);

        List<ReportRow> osReportRows = null;
        List<ReportRow> otReportRows = null;

        try (Connection connection = SQLUtility.getDbConnection()) {
            if (osRunSkey != null && !osRunSkey.isEmpty()) {
                osReportRows = processForRunSkey(connection, osRunSkey, batchType, msgCategory, msgCategoryString);
            }
            if (otRunSkey != null && !otRunSkey.isEmpty()) {
                otReportRows = processForRunSkey(connection, otRunSkey, batchType, msgCategory, msgCategoryString);
            }
        } catch (Exception e) {
            logger.error("Error during database operations: {}", e.getMessage(), e);
            throw e;
        }

//        String matchHeader = (osReportRows != null || otReportRows != null) ? "# " + Constants.getMatchHeaderSuffix(webServiceId, watchListType) + " "+ Constants.MATCHES : null;
        String matchHeader = "Specific Count";
        if (osReportRows != null) {
            writeSplitExcel(osReportRows, misDate, runNo, batchType, matchHeader, "OS", excelRowLimit);
        }
        if (otReportRows != null) {
            writeSplitExcel(otReportRows, misDate, runNo, batchType, matchHeader, "OT", excelRowLimit);
        }

    }

    private static List<ReportRow> processForRunSkey(Connection connection, String runSkey, String batchType, int msgCategory, String msgCategoryString) throws Exception {
        long startTime = System.currentTimeMillis();

        String batchTable = batchType.equalsIgnoreCase("ISO20022") ? "FCC_TF_XML_BATCH_TRXN" : "FCC_TF_ACH_BATCH_TRXN";

        String query = "SELECT " +
                       "b.N_GRP_MSG_ID, " +
                       "b.C_RAW_MSG, " +
                       "f.C_FEEDBACK_MESSAGE " +
                       "FROM " + batchTable + " b " +
                       "LEFT JOIN fcc_tf_feedback f ON b.N_GRP_MSG_ID = f.N_TRAX_TOKEN AND f.V_MSG_CATEGORY = ? " +
                       "WHERE b.N_RUN_SKEY = ? ";

        Set<Long> allTokens = new HashSet<>();
        Map<Long, String> tokenToRawMsg = new HashMap<>();
        Map<Long, JSONObject> feedbackMap = new HashMap<>();
        Map<Long, JSONObject> tokenToAdditionalDataMap = new HashMap<>();

        try (PreparedStatement pst = connection.prepareStatement(query)) {
            pst.setFetchSize(5000); // Increased fetch size for better performance
            pst.setString(1, msgCategoryString);
            pst.setLong(2, Long.parseLong(runSkey));
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    long token = rs.getLong("N_GRP_MSG_ID");

                    // Collect all unique tokens
                    allTokens.add(token);

                    // Raw Msg - store even if null, will be handled as empty string
                    if (!tokenToRawMsg.containsKey(token)) {
                        String rawMsg = rs.getString("C_RAW_MSG");
                        tokenToRawMsg.put(token, rawMsg != null ? rawMsg : "");
                        if (rawMsg == null) {
                            logger.debug("Token {} has null raw message, stored as empty string", token);
                        }
                    }

                    // Feedback - always store feedback data
                    String feedbackJson = rs.getString("C_FEEDBACK_MESSAGE");
                    if (feedbackJson != null && !feedbackJson.isEmpty()) {
                        feedbackMap.put(token, new JSONObject(feedbackJson));
                    } else {
                        // Always ensure feedback data exists
                        feedbackMap.put(token, new JSONObject("{\"message\": \"No feedback found\", \"matches\": []}"));
                        logger.debug("Token {} has no feedback data, stored default feedback", token);
                    }

                    // Additional Data - extract from raw message
                    String rawMsgStr = rs.getString("C_RAW_MSG");
                    if (rawMsgStr != null && !rawMsgStr.isEmpty() && !tokenToAdditionalDataMap.containsKey(token)) {
                        try {
                            JSONObject rawMessageObj = new JSONObject(rawMsgStr);
                            if (rawMessageObj.has("additionalData")) {
                                JSONObject additionalData = rawMessageObj.getJSONObject("additionalData");
                                tokenToAdditionalDataMap.put(token, additionalData);
                            } else {
                                // No additionalData field in raw message
                                tokenToAdditionalDataMap.put(token, new JSONObject());
                                logger.debug("Token {} has no additionalData field in raw message, initialized empty object", token);
                            }
                        } catch (Exception e) {
                            // Error parsing raw message JSON or extracting additionalData
                            tokenToAdditionalDataMap.put(token, new JSONObject());
                            logger.warn("Token {} failed to parse additional data from raw message: {}", token, e.getMessage());
                        }
                    } else if (!tokenToAdditionalDataMap.containsKey(token)) {
                        // Initialize empty additional data if not present
                        tokenToAdditionalDataMap.put(token, new JSONObject());
                        logger.debug("Token {} has no raw message, initialized empty additional data object", token);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in combined data fetch: {}", e.getMessage(), e);
            throw e;
        }

        List<Long> transactionTokens = new ArrayList<>(allTokens);

        if (transactionTokens.isEmpty()) {
            throw new Exception("No data found for runSkey: " + runSkey);
        }

        Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap = getBulkColumnNameWLS(connection, transactionTokens, msgCategory);

        long dbEndTime = System.currentTimeMillis();
        logger.info("DB queries took: {} ms", (dbEndTime - startTime));
        return analyzeResults(transactionTokens, tokenToResponseIdToColumnNamesMap, feedbackMap, tokenToAdditionalDataMap, runSkey, tokenToRawMsg);
    }

    private static List<ReportRow> analyzeResults(List<Long> transactionTokens, Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap,
                                                  Map<Long, JSONObject> feedbackMap, Map<Long, JSONObject> tokenToAdditionalDataMap,
                                                  String runSkey, Map<Long, String> tokenToRawMsg) {
        long startTime = System.currentTimeMillis();
        // Parallel processing of each trxn token
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ConcurrentLinkedQueue<ReportRow> queue = new ConcurrentLinkedQueue<>();
        for (long transactionToken : transactionTokens) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                String uid = "";
                String webService = "";
                String webServiceId = "";
                String watchListType = "";
                String tagName = "";
                String targetColumnName;
                String sourceInput = "";
                String targetInput = "";
                String messageKey = "";
                int ced = 0;
                try {
                    // Always process every token - no early returns
                    JSONObject eachResponse = feedbackMap.get(transactionToken);
                    JSONObject additionalData = tokenToAdditionalDataMap.get(transactionToken);

                    // Extract additional data if available
                    if (additionalData != null) {
                        uid = additionalData.optString(Constants.UID, "");
                        webService = additionalData.optString(Constants.WEBSERVICE, "");
                        webServiceId = additionalData.optString(Constants.WEBSERVICE_ID, "");
                        watchListType = additionalData.optString(Constants.WATCHLIST_TYPE, "");
                        tagName = additionalData.optString(Constants.TAGNAME, "");
                        targetColumnName = additionalData.optString(Constants.COLUMN, "");
                        sourceInput = additionalData.optString(Constants.VALUE, "");
                        targetInput = additionalData.optString(Constants.ORIGINAL_VALUE, "");
                        messageKey = additionalData.optString(Constants.MESSAGE_KEY_ADDITIONAL, "");
                        ced = additionalData.optInt(Constants.CED, 0);
                    } else {
                        targetColumnName = "";
                    }

                    // Check if we have feedback data and matches
                    boolean hasFeedback = eachResponse != null ;//&& eachResponse.has(Constants.MATCHES);
                    JSONArray matches = hasFeedback && eachResponse.has(Constants.MATCHES) ? eachResponse.getJSONArray(Constants.MATCHES) : new JSONArray();
                    int matchCount = matches.length();

                    String testStatus;
                    String comments;
                    int truePositives = 0;
                    boolean isColumnMismatch = false;
                    int filteredCount = 0;
                    String feedbackStatus = "";
                    String feedback = "";

                    if (!hasFeedback) {
                        // No feedback data - mark as FAIL
                        testStatus = Constants.FAIL;
                        comments = "No feedback data available";
                        feedbackStatus = eachResponse.optString(Constants.MATCHING_STATUS, "");
                        feedback = eachResponse.toString();
                        if (feedback.length() > 32767) {
                            feedback = "Value too large check feedback table";
                        }
                        logger.debug("Token {} has no feedback data, marking as FAIL", transactionToken);
                    } else if (matches.length() == 0) {
                        // Has feedback but no matches - mark as FAIL
                        testStatus = Constants.FAIL;
                        comments = "No matches found in feedback";
                        feedbackStatus = eachResponse.optString(Constants.MATCHING_STATUS, "");
                        feedback = eachResponse.toString();
                        if (feedback.length() > 32767) {
                            feedback = "Value too large check feedback table";
                        }
                        logger.debug("Token {} has feedback but no matches, marking as FAIL", transactionToken);
                    } else {
                        // Has feedback and matches - perform analysis
                        logger.debug("Token {} has {} matches, performing analysis", transactionToken, matches.length());

                        Map<Long, String> responseIdColumnNamesMap = tokenToResponseIdToColumnNamesMap.getOrDefault(transactionToken, Collections.emptyMap());

                        for (int i = 0; i < matches.length(); i++) {
                            JSONObject match = matches.getJSONObject(i);
                            String tagNameCsv = match.optString("tagName", "");
                            Set<String> tagNames = Arrays.stream(tagNameCsv.split(",")).map(String::trim).collect(Collectors.toSet());

                            String targetUid = match.getString(Constants.MATCHED_WATCHLIST_ID);
                            Long responseId = match.getLong(Constants.RESPONSE_ID);
                            String columnNameWls = responseIdColumnNamesMap.get(responseId);
                            Set<String> columnNames = columnNameWls != null ? Arrays.stream(columnNameWls.split(",")).collect(Collectors.toSet()) : Collections.emptySet();

                            // Filtered count for OS # ... matches
                            if (String.valueOf(match.optInt(Constants.WEBSERVICE_ID_FROM_MATCH)).equals(webServiceId) &&
                                    (!webServiceId.equals("3") && !webServiceId.equals("4") || match.optString("watchlistType").equalsIgnoreCase(watchListType))) {
                                filteredCount++;
                            }

                            boolean flag = uid.equals(targetUid)
                                    && watchListType.equalsIgnoreCase(match.optString("watchlistType"))
                                    && webServiceId.equalsIgnoreCase(String.valueOf(match.getInt(Constants.WEBSERVICE_ID_FROM_MATCH)))
                                    && tagNames.contains(tagName);

                            if (flag) {
                                if (columnNames.stream().anyMatch(col -> col.equalsIgnoreCase(targetColumnName))) { // Case-insensitive match
                                    truePositives++;
                                    isColumnMismatch = false;
                                    break; // Early exit if we only need count >=1
                                } else {
                                    isColumnMismatch = true;
                                }
                            }
                        }

                        testStatus = truePositives > 0 || isColumnMismatch ? Constants.PASS : Constants.FAIL;
                        feedbackStatus = eachResponse.optString(Constants.MATCHING_STATUS, "");
                        feedback = eachResponse.toString();
                        if (feedback.length() > 32767) {
                            feedback = "Value too large check feedback table";
                        }

                        if (Constants.PASS.equalsIgnoreCase(testStatus)) {
                            if (isColumnMismatch) comments = Constants.COLUMN_MISMATCH_COMMENT;
                            else comments = "";
                        } else {
                            comments = Constants.NO_MATCH_COMMENT;
                        }

                        logger.info("Status for transaction token {}: {} (True positives: {}, Filtered: {})",
                                  transactionToken, testStatus, truePositives, filteredCount);
                        if (Constants.FAIL.equalsIgnoreCase(testStatus)) {
                            logger.info("isColumnMismatch for token {}: {}", transactionToken, isColumnMismatch);
                        }
                    }

                    // Determine rule name based on CED
                    String type = "";
                    if (ced == 0) type = Constants.EXACT;
                    else if (ced > 0) type = Constants.FUZZY + ced + Constants.CED;
                    else if (ced == -1) type = "STOPWORD";
                    else if (ced == -2) type = "SYNONYM";

                    String ruleName = webService + " " + type;
                    String message = tokenToRawMsg.getOrDefault(transactionToken, "");

                    // Always create a ReportRow for every token
                    ReportRow row = new ReportRow(0, ruleName, message, tagName, sourceInput, targetInput,
                            targetColumnName, watchListType, uid, transactionToken, runSkey,
                            matchCount, feedbackStatus, filteredCount, feedback, testStatus, comments, messageKey, isColumnMismatch);
                    queue.add(row);

                    logger.debug("ReportRow created for token: {} with status: {}", transactionToken, testStatus);

                } catch (Exception e) {
                    logger.error("Error processing transaction token {}: {}", transactionToken, e.getMessage(), e);
                    // Even on error, create a minimal ReportRow to ensure token is included
                    try {
                        String message = tokenToRawMsg.getOrDefault(transactionToken, "");
                        ReportRow errorRow = new ReportRow(0, "ERROR", message, tagName, "", "", "",
                                watchListType, "", transactionToken, runSkey,
                                0, "ERROR", 0, "Processing failed: " + e.getMessage(),
                                Constants.FAIL, "Processing error", "", false);
                        queue.add(errorRow);
                        logger.warn("Created error ReportRow for token: {}", transactionToken);
                    } catch (Exception inner) {
                        logger.error("Failed to create error ReportRow for token: {}", transactionToken, inner);
                    }
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
//        reportRows.sort(Comparator.comparingLong(rr -> rr.transactionToken));
        long endTime = System.currentTimeMillis();
        logger.info("Analysis processing took: {} ms", (endTime - startTime));
        return reportRows;
    }

    private static void writeSplitExcel(List<ReportRow> reportRows, String misDate, String runNo, String batchType, String matchHeader, String type, int rowLimit) throws IOException {
        if (reportRows == null || reportRows.isEmpty()) return;
        long startTime = System.currentTimeMillis();

        String prefix;
        if ("ISO20022".equalsIgnoreCase(batchType)) {
            prefix = misDate + "_RUN" + runNo + "_STG_ANALYSIS_" + type;
        } else if ("NACHA".equalsIgnoreCase(batchType)) {
            prefix = misDate + "_RUN" + runNo + "_ACH_ANALYSIS_" + type;
        } else {
            throw new IllegalArgumentException("Invalid batchType");
        }

        int fileCount = (int) Math.ceil((double) reportRows.size() / rowLimit);
        for (int i = 0; i < fileCount; i++) {
            int start = i * rowLimit;
            int end = Math.min(start + rowLimit, reportRows.size());
            List<ReportRow> chunk = reportRows.subList(start, end);

            String fileName = (fileCount > 1) ? prefix + "_" + (i + 1) + Constants.XLSX_EXT : prefix + Constants.XLSX_EXT;

            try (SXSSFWorkbook wb = new SXSSFWorkbook(100)) { // Streaming workbook, keep 100 rows in memory
                Sheet sheet = wb.createSheet(type);

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

                CellStyle highlightYellow = wb.createCellStyle();
                highlightYellow.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
                highlightYellow.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                highlightYellow.setFont(boldFont);

                String[] headers = {
                        Constants.SEQ_NO,
                        Constants.RULE,
                        Constants.MESSAGE,
                        Constants.TAG,
                        Constants.SOURCE_INPUT,
                        Constants.TARGET_INPUT,
                        Constants.TARGET_COLUMN,
                        Constants.WATCHLIST,
                        Constants.NUID,
                        Constants.TRXN_TOKEN,
                        Constants.RUN_SKEY,
                        Constants.MATCH_COUNT,
                        Constants.FEEDBACK_STATUS,
                        matchHeader,
                        Constants.FEEDBACK,
                        Constants.TEST_STATUS,
                        Constants.COMMENTS,
                        Constants.MESSAGE_KEY
                };
                Row headerRow = sheet.createRow(0);
                for (int j = 0; j < headers.length; j++) {
                    headerRow.createCell(j).setCellValue(headers[j]);
                }

                int rowNum = 1;
                for (ReportRow rr : chunk) {
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
                    row.createCell(9).setCellValue(rr.transactionToken);
                    row.createCell(10).setCellValue(rr.runSkey);
                    row.createCell(11).setCellValue(rr.matchCount);
                    row.createCell(12).setCellValue(rr.feedbackStatus);
                    row.createCell(13).setCellValue(rr.specificMatches);
                    row.createCell(14).setCellValue(rr.feedback);
                    row.createCell(15).setCellValue(rr.testStatus);

                    if (Constants.PASS.equalsIgnoreCase(rr.testStatus)) {
                        if (rr.isColumnMismatch) {
                            row.getCell(15).setCellStyle(highlightYellow);
                        } else {
                            row.getCell(15).setCellStyle(highlightGreen);
                        }
                    } else  {
                        row.getCell(15).setCellStyle(highlightRed);
                    }

                    row.createCell(16).setCellValue(rr.comments);
                    row.createCell(17).setCellValue(rr.messageKey);
                    rowNum++;
                }

                File outputFile = new File(Constants.OUTPUT_FOLDER, fileName);
                try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                    wb.write(fos);
                }
                logger.info("Excel report generated at: {}", outputFile.getAbsolutePath());
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("Excel writing took: {} ms", (endTime - startTime));
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
                pst.setFetchSize(5000); // Increased fetch size for better performance
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
