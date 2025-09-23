package com.oracle.ofss.sanctions.tf.app;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Constants {
    // Database Table Mappings for Watchlists
    public static final Map<String, String> TABLE_WL_MAP;
    static {
        Map<String, String> map = new HashMap<>();
        map.put("COUNTRY", "FCC_TF_DIM_COUNTRY");
        map.put("CITY", "FCC_TF_DIM_CITY");
        map.put("GOODS", "FCC_TF_DIM_GOODS");
        map.put("PORT", "FCC_TF_DIM_PORT");
        map.put("STOP_KEYWORDS", "FCC_TF_DIM_STOPKEYWORDS");
        map.put("IDENTIFIER", "FCC_DIM_IDENTIFIER");
        map.put("WCPREM", "FCC_WL_WC_PREMIUM");
        map.put("WCSTANDARD", "FCC_WL_WC_STANDARD");
        map.put("DJW", "FCC_WL_DJW");
        map.put("OFAC", "FCC_WL_OFAC");
        map.put("HMT", "FCC_WL_HMT");
        map.put("EU", "FCC_WL_EUROPEAN_UNION");
        map.put("UN", "FCC_WL_UN");
        map.put("PRV_WL1", "FCC_WL_PRIVATELIST");

        TABLE_WL_MAP = Collections.unmodifiableMap(map); // Make it read-only
    }

    // Web Service Mappings
    public static final Map<String, String> WEBSERVICE_MAP;
    static {
        Map<String, String> map = new HashMap<>();
        map.put("1", "NameAndAddress");
        map.put("2", "Identifier");
        map.put("5", "Port");
        map.put("6", "Goods");

        WEBSERVICE_MAP = Collections.unmodifiableMap(map); // Make it read-only
    }

    // Encoding Configuration
    public static String ENCODER = "UTF-8";

    // Property Keys for Configuration
    public static String GENERATOR_BATCH_TYPE = "generator.batchType";
    public static String ANALYZER_BATCH_TYPE = "analyzer.batchType";
    public static String MIS_DATE = "misDate";
    public static String RUN_NO = "runNo";
    public static String TAGNAME = "tagName";
    public static String WEBSERVICE = "webService";
    public static String OS_RUN_SKEY = "analyzer.openSearch.runSkey";
    public static String OT_RUN_SKEY = "analyzer.oracleText.runSkey";
    public static String EXACT = "Exact";
    public static String FUZZY = "Fuzzy - ";
    public static String WATCHLIST_TYPE = "watchListType";
    public static String MATCHING_STATUS = "status";
    public static String MATCHING_COUNT = "matchCount";
    public static String WEBSERVICE_ID = "webServiceId";


    // CED properties
    public static String CED1 =  "ced1";
    public static String CED2 =  "ced2";
    public static String CED3 =  "ced3";

    // Database
    public static String JDBC_URL =  "jdbcurl";
    public static String JDBC_DRIVER =  "jdbcdriver";
    public static String WALLET_NAME =  "walletName";
    public static String WHERE_CLAUSE =  "whereClause";
    public static String REPLACE_SRC =  "replace.src";
    public static String REPLACE_TARGET_COLUMN =  "replace.targetColumn";
    public static String CONNECTION_ESTABLISHED =  "Connection established successfully!";
    public static String TNS_ADMIN =  "oracle.net.tns_admin";

    // JSON keys
    public static String UID =  "uid";
    public static String COLUMN =  "column";
    public static String TOKEN =  "token";
    public static String VALUE =  "value";
    public static String ORIGINAL_VALUE =  "originalValue";
    public static String CED =  "ced";
    public static String IDEN_TOKEN =  "identifierToken";
    public static String IDEN_VALUE =  "identifierValue";
    public static String IDEN_PREFIX =  "ID";
    public static String IS_STOPWORD_PRESENT = "isStopwordPresent";
    public static String LOOKUP_ID = "lookupId";
    public static String LOOKUP_VALUE_ID = "lookupValueId";
    public static String MESSAGE_KEY_ADDITIONAL = "messageKey";

    // Date formats
    public static String DATE_TIME_FORMAT = "ddMMyyHHmmss";

    // File names and paths
    public static String SOURCE_FILE_NAME = "source";
    public static String CONFIG_FILE_NAME = "config";
    public static String OUTPUT_FOLDER_NAME = "out";
    public static String BIN_FOLDER_NAME = "bin";
    public static String CURRENT_DIRECTORY = System.getProperty("user.dir");
    public static File PARENT_DIRECTORY = new File(CURRENT_DIRECTORY).getParentFile();
    public static String SOURCE_FILE_PATH = PARENT_DIRECTORY+File.separator+BIN_FOLDER_NAME+File.separator+SOURCE_FILE_NAME+".json";
    public static String CONFIG_FILE_PATH = PARENT_DIRECTORY+File.separator+BIN_FOLDER_NAME+File.separator+CONFIG_FILE_NAME+".properties";
    public static String COMMON_CONFIG_FILE_PATH = PARENT_DIRECTORY+File.separator+BIN_FOLDER_NAME+File.separator+"common.properties";
    public static String BIN_DIR_PATH = PARENT_DIRECTORY+File.separator+BIN_FOLDER_NAME;
    public static File OUTPUT_FOLDER = new File(PARENT_DIRECTORY, OUTPUT_FOLDER_NAME);

    // Excel splitting configuration
    public static String JSON_OBJJECT_LIMIT = "jsonObjectLimit";
    public static int DEFAULT_ROW_LIMIT = 1000;
    public static String EXCEL_ROW_LIMIT = "excelRowLimit";
    public static int DEFAULT_EXCEL_ROW_LIMIT = 1000;
    public static String FILE_NAME_LIST = "filename.txt";
    public static String RUN_DETAILS_FILE_NAME = "run_details.json";

    // Status strings
    public static String PASS = "PASS";
    public static String FAIL = "FAIL";
    public static String YES = "Y";
    public static String NO = "N";

    // Excel headers
    public static String SEQ_NO = "SeqNo";
    public static String RULE = "Rule Name";
    public static String MESSAGE = "Message ";
    public static String TAG = "Tag";
    public static String SOURCE_INPUT = "Source Input";
    public static String TARGET_INPUT = "Target Input";
    public static String TARGET_COLUMN = "Target Column";
    public static String WATCHLIST = "Watchlist";
    public static String NUID = "N_UID";
    public static String TRXN_TOKEN = "Transaction Token";
    public static String RUN_SKEY = "Run Skey";
    public static String MATCH_COUNT = "Match Count";
    public static String FEEDBACK_STATUS = "Feedback Status";
    public static String FEEDBACK = "Feedback";
    public static String TEST_STATUS = "Test Status";
    public static final String COMMENTS = "Comments";
    public static final String MESSAGE_KEY = "Message Key";
    public static final String OS_SHEET_NAME = "Open Search";
    public static final String OT_SHEET_NAME = "Oracle Text";

    // Comments
    public static final String COLUMN_MISMATCH_COMMENT = "Column name didn't match";
    public static final String NO_MATCH_COMMENT = "No Match";

    // Additional constants for cleanup
    public static final String DEFAULT_CONFIG_BASE = "config";
    public static final String XLSX_EXT = ".xlsx";
    public static final String JSON_EXT = ".json";
    public static final String MATCHES = "matches";
    public static final String MATCHED_WATCHLIST_ID = "matchedWatchlistId";
    public static final String RESPONSE_ID = "responseID";
    public static final String ISO20022 = "ISO20022";
    public static final String SEPA = "SEPA";
    public static final String NACHA = "NACHA";
    public static final int THREE = 3;
    public static final int FOUR = 4;
    public static final String WEBSERVICE_ID_FROM_MATCH = "webServiceID";
    public static final String INSERT_CHAR = "X";
}
