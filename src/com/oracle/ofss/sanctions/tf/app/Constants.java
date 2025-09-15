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

    // Module Configuration Flags
    public static final String TOGGLE_MATCHING_ENGINE = "toggleMatchingEngine";

    // Property Keys for Configuration
    public static String BATCH_TYPE = "batchtype";
    public static String MIS_DATE = "misdate";
    public static String RUN_NO = "runNo";
    public static String TAGNAME = "tagName";
    public static String WEBSERVICE = "webService";
    public static String EXACT = "Exact";
    public static String FUZZY = "Fuzzy - ";
    public static String WATCHLIST_TYPE = "watchListType";
    public static String TRANSACTION_SERVICE = "msgPosting.transactionService";
    public static String TOKEN_URL = "msgPosting.tokenUrl";
    public static String CLIENT_ID = "msgPosting.client.id";
    public static String CLIENT_SECRET = "msgPosting.client.secret";
    public static String DEVCORP7 = "msgPosting.devcorp7";
    public static String NAMESPACE = "msgPosting.namespace";
    public static String RETRY_REQUIRED_FLAG = "msgPosting.retryRequiredFlag";
    public static String RETRY_MAX_COUNT = "msgPosting.retryMaxCount";
    public static String RETRY_REFRESH_INTERVAL = "msgPosting.bearerTokenRefreshInterval";
    public static String RESTART_FLAG = "msgPosting.restartFlag";
    public static String POSTING_ENDPOINT = "-transaction-service/sync/process";
    public static String TRANSACTION_TOKEN = "transactionToken";
    public static String FEEDBACK_DATA = "feedbackData";
    public static String MATCHING_STATUS = "status";
    public static String MATCHING_COUNT = "matchCount";
    public static String WEBSERVICE_ID = "webServiceId";
    public static final String COMMENTS = "Comments";

    // CED properties
    public static String CED1 =  "ced1";
    public static String CED2 =  "ced2";
    public static String CED3 =  "ced3";

    // Database properties
    public static String JDBC_URL =  "jdbcurl";
    public static String JDBC_DRIVER =  "jdbcdriver";
    public static String WALLET_NAME =  "walletName";
    public static String WHERE_CLAUSE =  "whereClause";
    public static String REPLACE_SRC =  "replace.src";
    public static String REPLACE_TARGET_COLUMN =  "replace.targetColumn";

    // JSON keys
    public static String ADDITIONAL_DATA =  "additionalData";
    public static String TABLE =  "table";
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

    // Date formats
    public static String DATE_FORMAT =  "yyyy-MM-dd HH:mm:ss.SSS";
    public static String DATE_SUFFIX_FORMAT = "ddMMyy";
    public static String TIME_SUFFIX_FORMAT = "HHmmss";

    // Messages
    public static String WAIT_MSG =  "Wait for a while...It's gonna finish";
    public static String HOLD_ON_MSG_1 =  "Hold on...It's almost completed";
    public static String HOLD_ON_MSG_2 =  "Looking for Tortoise...Will find it soon";
    public static String SUCCESS_MSG =  "Heyy...Here it is";
    public static String LOAD_MSG =  "zzzz...on the way";

    // File names and paths
    public static String SOURCE_FILE_NAME = "source";
    public static String CONFIG_FILE_NAME = "config";
    public static String OUTPUT_FILE_NAME = "executing";
    public static String OUTPUT_FOLDER_NAME = "out";
    public static String BIN_FOLDER_NAME = "bin";
    public static String CURRENT_DIRECTORY = System.getProperty("user.dir");
    public static File PARENT_DIRECTORY = new File(CURRENT_DIRECTORY).getParentFile();
    public static String SOURCE_FILE_PATH = PARENT_DIRECTORY+File.separator+BIN_FOLDER_NAME+File.separator+SOURCE_FILE_NAME+".json";
    public static String CONFIG_FILE_PATH = PARENT_DIRECTORY+File.separator+BIN_FOLDER_NAME+File.separator+CONFIG_FILE_NAME+".properties";
    public static File OUTPUT_FOLDER = new File(PARENT_DIRECTORY, OUTPUT_FOLDER_NAME);

    // Excel splitting configuration
    public static String EXCEL_SPLIT_ROW_LIMIT = "excel.split.rowLimit";
    public static int DEFAULT_ROW_LIMIT = 1000;
    public static String OUTPUT_FILE_NAME_PATTERN = OUTPUT_FILE_NAME+"_%d";
    public static String FILE_NAME_LIST = "filename.txt";

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
    public static String MATCH_COUNT = "Match Count";
    public static String STATUS = "Status";
    public static String FEEDBACK_STATUS = "Feedback Status";
    public static String TEST_STATUS = "Test Status";

    // Comments
    public static final String COLUMN_MISMATCH_COMMENT = "Column name didn't match";
    public static final String NO_MATCH_COMMENT = "No Match";

    // Additional constants for cleanup
    public static final String DEFAULT_CONFIG_BASE = "config";
    public static final String XLSX_EXT = ".xlsx";
    public static final String JSON_EXT = ".json";
    public static final int MIN_ARGS = 6;
    public static final int DEFAULT_RETRY_MAX = 5;
    public static final long DEFAULT_REFRESH_INTERVAL_MIN = 30;
    public static final long THREAD_SLEEP_MS = 5000;
    public static final String CONTENT_TYPE_JSON = "application/json";
    public static final String AUTH_BEARER_PREFIX = "Bearer ";
    public static final int SUCCESS_CODE = 200;
    public static final int BAD_GATEWAY = 502;
    public static final int GATEWAY_TIMEOUT = 504;
    public static final int SERVICE_UNAVAILABLE = 503;
    public static final int NO_CONTENT = 204;
    public static final String GRANT_TYPE = "client_credentials";
    public static final String SCOPE = "urn:opc:idm:__myscopes__";
    public static final String MATCHES = "matches";
    public static final String MATCHED_WATCHLIST_ID = "matchedWatchlistId";
    public static final String RESPONSE_ID = "responseID";

    // Concurrency constants
    public static final String PROCESSOR_THREADS = "processor_thread_count";
    public static final String ANALYZER_THREADS = "analyzer_thread_count";
    public static final String POISON_PILL = "POISON_PILL";
    public static final int DEFAULT_THREAD_COUNT = 2;
}
