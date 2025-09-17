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
        String watchListType = props.getProperty(Constants.WATCHLIST_TYPE);
        String webServiceId = props.getProperty(Constants.WEBSERVICE_ID);
        String webService = props.getProperty(Constants.WEBSERVICE);
        String tagName = props.getProperty(Constants.TAGNAME);
        String osRunSkey = props.getProperty(Constants.OS_RUN_SKEY);
        String otRunSkey = props.getProperty(Constants.OT_RUN_SKEY);
        String batchType = props.getProperty(Constants.BATCH_TYPE);
        int excelRowLimit = Integer.parseInt(props.getProperty(Constants.EXCEL_ROW_LIMIT, String.valueOf(Constants.DEFAULT_EXCEL_ROW_LIMIT)));

        int msgCategory = batchType.equalsIgnoreCase(Constants.ISO20022)?Constants.THREE:Constants.FOUR;
        String msgCategoryString = batchType.equalsIgnoreCase(Constants.ISO20022)?Constants.SEPA:Constants.NACHA;

        String misDate = props.getProperty(Constants.MIS_DATE);
        String runNo = props.getProperty(Constants.RUN_NO);

        List<ReportRow> osReportRows = null;
        List<ReportRow> otReportRows = null;

        try (Connection connection = SQLUtility.getDbConnection()) {
            if (osRunSkey != null && !osRunSkey.isEmpty()) {
                osReportRows = processForRunSkey(connection, osRunSkey, batchType, msgCategory, msgCategoryString, watchListType, webServiceId, webService, tagName);
            }
            if (otRunSkey != null && !otRunSkey.isEmpty()) {
                otReportRows = processForRunSkey(connection, otRunSkey, batchType, msgCategory, msgCategoryString, watchListType, webServiceId, webService, tagName);
            }
        } catch (Exception e) {
            logger.error("Error during database operations: {}", e.getMessage(), e);
            throw e;
        }

        String matchHeader = (osReportRows != null || otReportRows != null) ? "# " + Constants.getMatchHeaderSuffix(webServiceId, watchListType) + " "+ Constants.MATCHES : null;

        if (osReportRows != null) {
            writeSplitExcel(osReportRows, misDate, runNo, batchType, matchHeader, "OS", excelRowLimit);
        }
        if (otReportRows != null) {
            writeSplitExcel(otReportRows, misDate, runNo, batchType, matchHeader, "OT", excelRowLimit);
        }

    }

    private static List<ReportRow> processForRunSkey(Connection connection, String runSkey, String batchType, int msgCategory, String msgCategoryString, String watchListType, String webServiceId, String webService, String tagName) throws Exception {
        long startTime = System.currentTimeMillis();

        String batchTable = batchType.equalsIgnoreCase("ISO20022") ? "FCC_TF_XML_BATCH_TRXN" : "FCC_TF_ACH_BATCH_TRXN";
        String rawDataTable = msgCategory == 3 ? "fcc_tf_xml_raw_data" : "fcc_tf_ach_raw_data";

        String query = "SELECT " +
                       "b.N_GRP_MSG_ID, " +
                       "b.C_RAW_MSG, " +
                       "f.C_FEEDBACK_MESSAGE, " +
                       "r.C_ADDITIONAL_DATA " +
                       "FROM " + batchTable + " b " +
                       "LEFT JOIN fcc_tf_feedback f ON b.N_GRP_MSG_ID = f.N_TRAX_TOKEN AND f.V_MSG_CATEGORY = ? " +
                       "LEFT JOIN " + rawDataTable + " r ON b.N_GRP_MSG_ID = r.N_GRP_MSG_ID " +
                       "WHERE b.N_RUN_SKEY = ? ";

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

                    // Raw Msg
                    if (!tokenToRawMsg.containsKey(token)) {
                        tokenToRawMsg.put(token, rs.getString("C_RAW_MSG"));
                    }

                    // Feedback
                    String feedbackJson = rs.getString("C_FEEDBACK_MESSAGE");
                    if (feedbackJson != null && !feedbackJson.isEmpty()) {
                        feedbackMap.put(token, new JSONObject(feedbackJson));
                    } else if (!feedbackMap.containsKey(token)) {
                        feedbackMap.put(token, new JSONObject("No Feedback Found."));
                    }

                    // Additional Data
                    String additionalJson = rs.getString("C_ADDITIONAL_DATA");
                    if (additionalJson != null && !additionalJson.isEmpty() && !tokenToAdditionalDataMap.containsKey(token)) {
                        tokenToAdditionalDataMap.put(token, new JSONObject(additionalJson));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in combined data fetch: {}", e.getMessage(), e);
            throw e;
        }

        List<Long> transactionTokens = new ArrayList<>(tokenToRawMsg.keySet());

        if (transactionTokens.isEmpty()) {
            throw new Exception("No data found for runSkey: " + runSkey);
        }

        Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap = getBulkColumnNameWLS(connection, transactionTokens, msgCategory);

        long dbEndTime = System.currentTimeMillis();
        logger.info("DB queries took: {} ms", (dbEndTime - startTime));
        return analyzeResults(transactionTokens, tokenToResponseIdToColumnNamesMap, feedbackMap, tokenToAdditionalDataMap, watchListType, webServiceId, webService, tagName, runSkey, tokenToRawMsg);
    }

    private static List<ReportRow> analyzeResults(List<Long> transactionTokens, Map<Long, Map<Long, String>> tokenToResponseIdToColumnNamesMap,
                                                  Map<Long, JSONObject> feedbackMap, Map<Long, JSONObject> tokenToAdditionalDataMap,
                                                  String watchListType, String webServiceId, String webService, String tagName, String runSkey, Map<Long, String> tokenToRawMsg) {
        long startTime = System.currentTimeMillis();
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

                    boolean isColumnMismatch = false;
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
                        String testStatus = truePositives > 0 || isColumnMismatch? Constants.PASS : Constants.FAIL;

                        logger.info("Status for trxn token {} : {}",transactionToken,testStatus);
                        if (Constants.FAIL.equalsIgnoreCase(testStatus))
                            logger.info("isColumnMismatch : {}",isColumnMismatch);

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
                        if(Constants.PASS.equalsIgnoreCase(testStatus)){
                            if(isColumnMismatch) comments = Constants.COLUMN_MISMATCH_COMMENT;
                        } else comments = Constants.NO_MATCH_COMMENT;



                        ReportRow row = new ReportRow(0, ruleName, message, tagName, sourceInput, targetInput,
                                targetColumnName, watchListType, uid, transactionToken, runSkey,
                                matchCount, feedbackStatus, filteredCount, feedback, testStatus, comments, additionalData.optString("MessageKey", ""), isColumnMismatch);
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
