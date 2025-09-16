package com.oracle.ofss.sanctions.tf.app;

import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class RawMessageGenerator {
    private static final Logger logger = LoggerFactory.getLogger(RawMessageGenerator.class);

    private static class CustomEscapes extends CharacterEscapes {
        private final int[] asciiEscapes;

        public CustomEscapes() {
            asciiEscapes = CharacterEscapes.standardAsciiEscapesForJSON();
            asciiEscapes['\n'] = CharacterEscapes.ESCAPE_NONE;
            asciiEscapes['\r'] = CharacterEscapes.ESCAPE_NONE;
            asciiEscapes['\t'] = CharacterEscapes.ESCAPE_NONE;
        }

        @Override
        public int[] getEscapeCodesForAscii() {
            return asciiEscapes;
        }

        @Override
        public SerializableString getEscapeSequence(int ch) {
            return null;
        }
    }

    public static int generateRawMessage(BlockingQueue<File> queue, Properties props) throws Exception {
        long startTime = System.currentTimeMillis();
        logger.info("=============================================================");
        logger.info("                RAW MESSAGE GENERATOR STARTED                ");
        logger.info("=============================================================");
        Connection connection = null;
        List<SourceInputModel> rawMessages = null;
        ResultSet rs = null;

        try {

            String watchlistType = props.getProperty(Constants.WATCHLIST_TYPE);
            String tableName = Constants.TABLE_WL_MAP.get(watchlistType);
            String tagName = props.getProperty(Constants.TAGNAME);
            String webService = props.getProperty(Constants.WEBSERVICE);
            String webserviceId = props.getProperty(Constants.WEBSERVICE_ID);
            boolean isStopwordEnabled = Constants.YES.equalsIgnoreCase(props.getProperty("stopword"));
            boolean isSynonymEnabled = Constants.YES.equalsIgnoreCase(props.getProperty("synonym"));

            try {
                validateConfigProperties(watchlistType, webserviceId, isStopwordEnabled, isSynonymEnabled);
                logger.info("Config Properties Validation Passed.");
            } catch (Exception e){
                logger.info("Config Properties Validation Failed.");
                throw new Exception(e);
            }

            SourceInputModel sourceModel = loadJsonFromFile(Constants.SOURCE_FILE_PATH);
            logger.info("Loaded source model");


            connection = SQLUtility.getDbConnection();
            rs = prepareQueryAndGetTableData(connection, props, tableName);



            rawMessages = generateRawMessageJsonArray(rs, props, sourceModel, tableName, tagName, webserviceId, watchlistType, isStopwordEnabled, isSynonymEnabled);

            if (!rawMessages.isEmpty()) {
                writeRawMessagesToJsonFile(rawMessages, props);
            }

            logger.info("=============================================================");
            logger.info("                 RAW MESSAGE GENERATOR ENDED                 ");
            logger.info("=============================================================");
            long endTime = System.currentTimeMillis();

            logger.info("Time taken by Raw Message Generator: {} seconds", (endTime - startTime) / 1000L);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (connection != null) {
                try {
                    connection.close();
                    logger.info("Connection closed.");
                } catch (SQLException e) {
                    logger.error("Failed to close the connection:");
                    e.printStackTrace();
                }
            }
        }
        return rawMessages != null ? rawMessages.size() : 0;
    }

    private static boolean validateConfigProperties(String watchlistType, String webserviceId, boolean isStopwordEnabled, boolean isSynonymEnabled) throws Exception {
        // Check if both synonym and stopword are enabled
        if (isSynonymEnabled && isStopwordEnabled) {
            throw new Exception("Cannot enable both synonym and stopword at the same time. Only one should be enabled.");
        }

        // If neither synonym nor stopword is enabled, return true
        if (!isSynonymEnabled && !isStopwordEnabled) {
            return true;
        }

        // Define unsupported webserviceIds
        List<String> unsupportedWebserviceIds = Arrays.asList("2", "5", "6");
        if (unsupportedWebserviceIds.contains(webserviceId)) {
            throw new Exception("Synonym or Stopword are not supported for "+ Constants.WEBSERVICE_MAP.get(webserviceId));
        }

        // Define specific validation rules for webserviceId "3" and "4"
        if ("3".equalsIgnoreCase(webserviceId)) {
            validateWebserviceId3(watchlistType, isStopwordEnabled);
        } else if ("4".equalsIgnoreCase(webserviceId)) {
            validateWebserviceId4(watchlistType, isSynonymEnabled);
        }

        return true;
    }

    private static void validateWebserviceId3(String watchlistType, boolean isStopwordEnabled) throws Exception {
        if ("CITY".equalsIgnoreCase(watchlistType)) {
            throw new Exception("Synonym or Stopword are not supported for City");
        }
        if (isStopwordEnabled && "COUNTRY".equalsIgnoreCase(watchlistType)) {
            throw new Exception("Stopword is not supported for Country");
        }
    }

    private static void validateWebserviceId4(String watchlistType, boolean isSynonymEnabled) throws Exception {
        if ("IDENTIFIER".equalsIgnoreCase(watchlistType)) {
            throw new Exception("Synonym or Stopword are not supported for Narrative Identifier");
        }
        if (isSynonymEnabled) {
            List<String> unsupportedWatchlistTypes = Arrays.asList("CITY", "GOODS", "PORT", "STOP_KEYWORDS");
            if (unsupportedWatchlistTypes.contains(watchlistType.toUpperCase())) {
                throw new Exception("Synonym is not supported for Narrative " + watchlistType);
            }
        }
    }


    private static ResultSet prepareQueryAndGetTableData(Connection connection, Properties props, String tableName) throws Exception {
        PreparedStatement pst = null;
        ResultSet rs = null;
        String filter="";
        if(props.containsKey(Constants.WHERE_CLAUSE)){
            filter = " where "+ props.get(Constants.WHERE_CLAUSE);
        }

        String query = "select * from "+tableName+" "+filter;
        logger.info("SQL Query generated:: {}", query);
        try {
            pst = connection.prepareStatement(query);
            rs = pst.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Something went wrong while preparing Query: ", e);
        }
        return rs;
    }

    public static List<SourceInputModel> generateRawMessageJsonArray(ResultSet rs, Properties props, SourceInputModel sourceModel, String tableName, String tagName, String webserviceId, String watchlistType, boolean isStopwordEnabled, boolean isSynonymEnabled) throws Exception {
        List<SourceInputModel> rawMessages = new ArrayList<>();
        int maxIndex = getMaxIndex(props, Constants.REPLACE_SRC);
        SourceInputModel temp;
        int updatedCount = 0;

        LocalDateTime now = LocalDateTime.now();
        String dateTimeStr = now.format(DateTimeFormatter.ofPattern("ddMMyyHHmmss"));

        List<Object[]> stopwords = null;
        Map<String, Map<String, String>> synonymMap = null;

        if (isStopwordEnabled) {
            Connection connection = SQLUtility.getDbConnection();
            stopwords = getRelevantStopwords(props, connection);
            connection.close();
        }

        if (isSynonymEnabled) {
            Connection connection = SQLUtility.getDbConnection();
            synonymMap = loadSynonyms(connection, watchlistType);
            connection.close();
        }

        int cnt=0;
        while(rs.next()) {
            if (sourceModel != null) {
                for (int i = 1; i <= maxIndex; i++) {
                    String srcKey = Constants.REPLACE_SRC+"[" + i + "]";
                    String targetColumnKey = Constants.REPLACE_TARGET_COLUMN+"[" + i + "]";

                    String token = props.getProperty(srcKey);
                    String targetColumn = props.getProperty(targetColumnKey);
                    String tokenValue = rs.getString(targetColumn);
                    if(tokenValue==null) break;
                    String identifierToken =  props.getProperty(Constants.REPLACE_SRC+"[0]");
                    String identifierTargetColumn = props.getProperty(Constants.REPLACE_TARGET_COLUMN+"[0]");
                    String identifierToBeReplaced = rs.getString(identifierTargetColumn);
                    String uid = rs.getString(Constants.NUID);

                    String[] toBeReplacedValues = tokenValue.split(";");

                    for(String toBeReplaced : toBeReplacedValues) {
                        if (isSynonymEnabled) {
                            List<Map<String, Object>> variantsWithInfo = generateSynonymVariantsWithInfo(toBeReplaced, synonymMap, props);
                            for (Map<String, Object> info : variantsWithInfo) {
                                String variant = (String) info.get("variant");
                                String lookupIds = (String) info.get("lookupIds");
                                String lookupValueIds = (String) info.get("lookupValueIds");
                                temp = cloneSourceModel(sourceModel);
                                updatedCount = createRawMsg(temp, variant, identifierToBeReplaced, token, targetColumn, identifierToken, tableName, rawMessages, updatedCount, tokenValue, -2, uid, tagName, webserviceId, lookupIds, lookupValueIds, dateTimeStr);
                            }
                        } else {
                            if (!isStopwordEnabled) {
                                // 0 ced -> exact
                                temp = cloneSourceModel(sourceModel);
                                updatedCount = createRawMsg(temp, toBeReplaced, identifierToBeReplaced, token, targetColumn, identifierToken, tableName, rawMessages, updatedCount, tokenValue, 0, uid, tagName, webserviceId, "NA", "NA", dateTimeStr);

                                if (props.getProperty(Constants.CED1).equalsIgnoreCase(Constants.YES)) { // 1 ced
                                    List<String> oneCedList = generate1CedVariants(toBeReplaced);
                                    for (String value : oneCedList) {
                                        temp = cloneSourceModel(sourceModel);
                                        updatedCount = createRawMsg(temp, value, identifierToBeReplaced, token, targetColumn, identifierToken, tableName, rawMessages, updatedCount, tokenValue, 1, uid, tagName, webserviceId, "NA", "NA", dateTimeStr);
                                    }
                                }

                                if (props.getProperty(Constants.CED2).equalsIgnoreCase(Constants.YES)) { // 2 ced
                                    List<String> twoCedList = generate2CedVariants(toBeReplaced);
                                    for (String value : twoCedList) {
                                        temp = cloneSourceModel(sourceModel);
                                        updatedCount = createRawMsg(temp, value, identifierToBeReplaced, token, targetColumn, identifierToken, tableName, rawMessages, updatedCount, tokenValue, 2, uid, tagName, webserviceId, "NA", "NA", dateTimeStr);
                                    }
                                }

                                if (props.getProperty(Constants.CED3).equalsIgnoreCase(Constants.YES)) { // 3 ced
                                    List<String> threeCedList = generate3CedVariants(toBeReplaced);
                                    for (String value : threeCedList) {
                                        temp = cloneSourceModel(sourceModel);
                                        updatedCount = createRawMsg(temp, value, identifierToBeReplaced, token, targetColumn, identifierToken, tableName, rawMessages, updatedCount, tokenValue, 3, uid, tagName, webserviceId, "NA", "NA", dateTimeStr);
                                    }
                                }
                            }

                            // Stopword variants
                            if (isStopwordEnabled && stopwords != null && !stopwords.isEmpty()) {
                                for (Object[] pair : stopwords) {
                                    String stop = (String) pair[0];
                                    String lookupId = (String) pair[1];
                                    String lookupValueId = (String) pair[2];
                                    List<String> variants = generateStopwordVariants(toBeReplaced, stop);
                                    for (String variant : variants) {
                                        temp = cloneSourceModel(sourceModel);
                                        updatedCount = createRawMsg(temp, variant, identifierToBeReplaced, token, targetColumn, identifierToken, tableName, rawMessages, updatedCount, tokenValue, -1, uid, tagName, webserviceId, lookupId, lookupValueId, dateTimeStr);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            cnt++;
        }
        logger.info("No. of rows selected from Watchlist:: {}", cnt);
        logger.info("No. of raw message created by Generator:: {}", updatedCount);
        return rawMessages;

    }

    private static SourceInputModel cloneSourceModel(SourceInputModel original) {
        return new SourceInputModel(
                original.getRawMessage(),
                original.getBusinessDomainCode(),
                original.getJurisdictionCode(),
                original.getMessageDirection(),
                new HashMap<>(original.getAdditionalData())
        );
    }

public static int createRawMsg(SourceInputModel temp, String value, String identifierToBeReplaced,
                                   String token, String targetColumn, String identifierToken,
                                   String tableName, List<SourceInputModel> rawMessages, int updatedCount, String originalValue, int ced, String uid,
                                   String tagName, String webserviceId, String lookupIds, String lookupValueIds, String dateTimeStr){
        if (value != null) {
            logger.info("toBeReplaced: {} originalValue: {}  token: {}  column: {}  identifier: {} ced: {}", value, originalValue, token, targetColumn, identifierToBeReplaced, ced);
            identifierToBeReplaced = Constants.IDEN_PREFIX + identifierToBeReplaced;

            String raw = temp.getRawMessage();
            raw = raw.replace(token, value);
            raw = raw.replace(identifierToken, identifierToBeReplaced);
            temp.setRawMessage(raw);

            Map<String, Object> additionalData = temp.getAdditionalData();
            additionalData.put(Constants.TABLE, tableName);
            additionalData.put(Constants.UID, uid);
            additionalData.put(Constants.COLUMN, targetColumn);
            additionalData.put(Constants.TOKEN, token);
            additionalData.put(Constants.VALUE, value);
            additionalData.put(Constants.ORIGINAL_VALUE, originalValue);
            additionalData.put(Constants.CED, ced);
            additionalData.put(Constants.TAGNAME, tagName);
            additionalData.put(Constants.WEBSERVICE_ID, webserviceId);
            additionalData.put(Constants.IDEN_TOKEN, identifierToken);
            additionalData.put(Constants.IDEN_VALUE, identifierToBeReplaced);
            additionalData.put(Constants.IS_STOPWORD_PRESENT, (ced == -1 ? "Y" : "N"));
            additionalData.put("isSynonymPresent", (ced == -2 ? "Y" : "N"));
            additionalData.put(Constants.LOOKUP_ID, lookupIds);
            additionalData.put(Constants.LOOKUP_VALUE_ID, lookupValueIds);

            String messageKey = dateTimeStr + (updatedCount + 1);
            additionalData.put("MessageKey", messageKey);

            rawMessages.add(temp);

            updatedCount++;
        }
        return updatedCount;
    }
    public static List<String> generate1CedVariants(String input) {
        List<String> variants = new ArrayList<>();
        int len = input.length();

        // Delete
        if (len >= 1) variants.add(input.substring(1)); // remove first
        if (len >= 3) variants.add(input.substring(0, len / 2) + input.substring((len / 2) + 1)); // remove middle
        if (len >= 1) variants.add(input.substring(0, len - 1)); // remove last

//        // Insert
//        variants.add(INSERT_CHAR + input); // insert at start
//        variants.add(input.substring(0, len / 2) + INSERT_CHAR + input.substring(len / 2)); // middle
//        variants.add(input + INSERT_CHAR); // insert at end

        return variants;
    }

    private static List<Object[]> getRelevantStopwords(Properties props, Connection connection) throws SQLException {
        List<Object[]> stopwords = new ArrayList<>();

        String query = "SELECT * FROM ( " +
                "  SELECT v.V_LOOKUP_VALUES, v.N_LOOKUP_ID, v.N_LOOKUP_VALUE_ID, " +
                "         ROW_NUMBER() OVER (PARTITION BY l.N_LOOKUP_ID ORDER BY DBMS_RANDOM.VALUE) AS rn " +
                "  FROM fcc_idx_m_lookup l " +
                "  JOIN FCC_IDX_M_LOOKUP_VALUES v ON l.N_LOOKUP_ID = v.N_LOOKUP_ID " +
                "  WHERE l.F_IS_SYNONYM = 'N' " +
                "  AND l.N_LOOKUP_ID in ("+props.getProperty("stopword.lookupIdIn")+")"+
                " ) WHERE rn <= ?";

        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setString(1, props.getProperty("stopword.pickValuesFromEachLookup"));
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            String value = rs.getString("V_LOOKUP_VALUES");
            String lookupId = rs.getString("N_LOOKUP_ID");
            String lookupValueId = rs.getString("N_LOOKUP_VALUE_ID");
            stopwords.add(new Object[]{value, lookupId, lookupValueId});
        }
        rs.close();
        stmt.close();

        return stopwords;
    }

    private static Map<String, Map<String, String>> loadSynonyms(Connection connection, String watchlistType) throws SQLException {
        Map<String, Map<String, String>> synonymMap = new HashMap<>();
        List<String> lookupIds = getLookupIdsForWatchlistType(watchlistType);

        if (lookupIds.isEmpty()) {
            return synonymMap; // Return empty map if no lookup IDs are specified for the watchlist type
        }

        String query = "SELECT N_LOOKUP_ID FROM fcc_idx_m_lookup WHERE F_IS_SYNONYM = 'Y' AND N_LOOKUP_ID IN (" + String.join(",", lookupIds) + ")";

        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String lookupId = rs.getString("N_LOOKUP_ID");
                Map<String, String> innerMap = new HashMap<>();
                String valuesQuery = "SELECT N_LOOKUP_VALUE_ID, V_LOOKUP_VALUES FROM FCC_IDX_M_LOOKUP_VALUES WHERE N_LOOKUP_ID = ?";
                try (PreparedStatement vStmt = connection.prepareStatement(valuesQuery)) {
                    vStmt.setString(1, lookupId);
                    try (ResultSet vRs = vStmt.executeQuery()) {
                        while (vRs.next()) {
                            String valueId = vRs.getString("N_LOOKUP_VALUE_ID");
                            String values = vRs.getString("V_LOOKUP_VALUES");
                            // Ignore (remove) newline characters in values
                            if (values != null) {
                                values = values.replaceAll("\\n", "");
                            }
                            innerMap.put(valueId, values);
                        }
                    }
                }
                synonymMap.put(lookupId, innerMap);
            }
        }
        return synonymMap;
    }

    private static List<String> getLookupIdsForWatchlistType(String watchlistType) {
        List<String> lookupIds = new ArrayList<>();
        switch (watchlistType) {
            case "COUNTRY":
                lookupIds.add("2");
                break;
            case "WCPREM":
            case "OFAC":
            case "DJW":
            case "PRV_WL1":
            case "WCSTANDARD":
            case "EU":
            case "HMT":
            case "UN":
                lookupIds.add("1");
                lookupIds.add("3");
                lookupIds.add("6");
                break;
            // Add more cases as needed for other watchlist types
            default:
                break;
        }
        return lookupIds;
    }

    private static List<Map<String, Object>> generateSynonymVariantsWithInfo(String toBeReplaced, Map<String, Map<String, String>> synonymMap, Properties props) {
        List<Map<String, Object>> result = new ArrayList<>();
        boolean multiword = "Y".equalsIgnoreCase(props.getProperty("synonym.multiword", "N"));
        boolean multipleGroups = "Y".equalsIgnoreCase(props.getProperty("synonym.multipleGroups", "N"));

        if (!multiword) {
            Set<String> usedLookupIds = new HashSet<>();
            Set<String> usedValueIds = new HashSet<>();
            List<String> alts = new ArrayList<>();
            boolean found = false;
            outer: for (Map.Entry<String, Map<String, String>> lookupEntry : synonymMap.entrySet()) {
                String lookupId = lookupEntry.getKey();
                for (Map.Entry<String, String> valueEntry : lookupEntry.getValue().entrySet()) {
                    String valueId = valueEntry.getKey();
                    String valuesStr = valueEntry.getValue();
                    String[] synonyms = valuesStr.split(",");
                    if (Arrays.asList(synonyms).contains(toBeReplaced)) {
                        found = true;
                        usedLookupIds.add(lookupId);
                        usedValueIds.add(valueId);
                        for (String alt : synonyms) {
                            if (!alt.equals(toBeReplaced) && !alts.contains(alt)) {
                                alts.add(alt);
                            }
                        }
                        if (!multipleGroups) break outer;
                    }
                }
            }
            if (!alts.isEmpty()) {
                String lids = String.join(",", usedLookupIds);
                String vids = String.join(",", usedValueIds);
                for (String alt : alts) {
                    Map<String, Object> info = new HashMap<>();
                    info.put("variant", alt);
                    info.put("lookupIds", lids);
                    info.put("lookupValueIds", vids);
                    result.add(info);
                }
            }
        } else {
            String[] words = toBeReplaced.split("\\s+");
            List<List<String>> options = new ArrayList<>();
            List<Set<String>> lidsPer = new ArrayList<>();
            List<Set<String>> vidsPer = new ArrayList<>();
            for (String word : words) {
                List<String> wordOptions = new ArrayList<>();
                wordOptions.add(word);
                Set<String> wordLids = new HashSet<>();
                Set<String> wordVids = new HashSet<>();
                outer: for (Map.Entry<String, Map<String, String>> lookupEntry : synonymMap.entrySet()) {
                    String lookupId = lookupEntry.getKey();
                    for (Map.Entry<String, String> valueEntry : lookupEntry.getValue().entrySet()) {
                        String valueId = valueEntry.getKey();
                        String valuesStr = valueEntry.getValue();
                        String[] synonyms = valuesStr.split(",");
                        if (Arrays.asList(synonyms).contains(word)) {
                            wordLids.add(lookupId);
                            wordVids.add(valueId);
                            for (String alt : synonyms) {
                                if (!alt.equals(word) && !wordOptions.contains(alt)) {
                                    wordOptions.add(alt);
                                }
                            }
                            if (!multipleGroups) break outer;
                        }
                    }
                }
                options.add(wordOptions);
                lidsPer.add(wordLids);
                vidsPer.add(wordVids);
            }
            generateCombinations(options, lidsPer, vidsPer, words, 0, new ArrayList<>(), new HashSet<>(), new HashSet<>(), result, toBeReplaced, new HashSet<>());
        }
        return result;
    }

    private static void generateCombinations(List<List<String>> options, List<Set<String>> lidsPer, List<Set<String>> vidsPer, String[] originalWords, int index, List<String> current, Set<String> currentLids, Set<String> currentVids, List<Map<String, Object>> result, String original, Set<String> seenVariants) {
        if (index == options.size()) {
            String variant = String.join(" ", current);
            if (!variant.equals(original) && seenVariants.add(variant)) {
                String lidsStr = currentLids.isEmpty() ? "NA" : String.join(",", currentLids);
                String vidsStr = currentVids.isEmpty() ? "NA" : String.join(",", currentVids);
                Map<String, Object> info = new HashMap<>();
                info.put("variant", variant);
                info.put("lookupIds", lidsStr);
                info.put("lookupValueIds", vidsStr);
                result.add(info);
            }
            return;
        }

        for (String choice : options.get(index)) {
            current.add(choice);
            Set<String> newLids = new HashSet<>(currentLids);
            Set<String> newVids = new HashSet<>(currentVids);
            if (!choice.equals(originalWords[index])) {
                newLids.addAll(lidsPer.get(index));
                newVids.addAll(vidsPer.get(index));
            }
            generateCombinations(options, lidsPer, vidsPer, originalWords, index + 1, current, newLids, newVids, result, original, seenVariants);
            current.remove(current.size() - 1);
        }
    }

    public static List<String> generateStopwordVariants(String originalValue, String stop) {
        List<String> variants = new ArrayList<>();
        String[] words = originalValue.split("\\s+");
        int numWords = words.length;

        // Prefix
        variants.add(stop + " " + originalValue);

        // Suffix
        variants.add(originalValue + " " + stop);

        // Between
        if (numWords > 1) {
            for (int pos = 1; pos < numWords; pos++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < pos; j++) {
                    sb.append(words[j]).append(" ");
                }
                sb.append(stop).append(" ");
                for (int j = pos; j < numWords; j++) {
                    sb.append(words[j]).append(" ");
                }
                variants.add(sb.toString().trim());
            }
        }
        return variants;
    }

    public static List<String> generate2CedVariants(String input) {
        List<String> variants = new ArrayList<>();
        int len = input.length();

        // Delete 2 characters
        if (len >= 3) {
            variants.add(input.substring(2)); // remove first two
            variants.add(input.substring(0, len / 2 - 1) + input.substring((len / 2) + 1)); // remove around middle
            variants.add(input.substring(0, len - 2)); // remove last two
        }

//        // Insert 2 characters
//        variants.add(INSERT_CHAR + INSERT_CHAR + input); // insert two at start
//        variants.add(input.substring(0, len / 2) + INSERT_CHAR + INSERT_CHAR + input.substring(len / 2)); // middle
//        variants.add(input + INSERT_CHAR + INSERT_CHAR); // insert two at end

        return variants;
    }

    public static List<String> generate3CedVariants(String input) {
        List<String> variants = new ArrayList<>();
        int len = input.length();

        // Delete 3 characters
        if (len >= 4) {
            variants.add(input.substring(3)); // remove first 3
            variants.add(input.substring(0, len / 2 - 1) + input.substring((len / 2) + 2)); // remove around middle
            variants.add(input.substring(0, len - 3)); // remove last 3
        }

//        // Insert 3 characters
//        variants.add("" + INSERT_CHAR + INSERT_CHAR + INSERT_CHAR + input); // insert 3 at start
//        variants.add(input.substring(0, len / 2) + INSERT_CHAR + INSERT_CHAR + INSERT_CHAR + input.substring(len / 2)); // middle
//        variants.add(input + INSERT_CHAR + INSERT_CHAR + INSERT_CHAR); // end

        return variants;
    }

    private static int getMaxIndex(Properties props, String prefix) throws Exception {
        int maxIndex = 0;
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix + "[")) {
                try {
                    int index = Integer.parseInt(key.substring(prefix.length() + 1, key.length() - 1));
                    maxIndex = Math.max(maxIndex, index);
                } catch (NumberFormatException e) {
                    throw new Exception("Something went wrong while getting maxIndex",e);
                }
            }
        }
        return maxIndex;
    }

    public static SourceInputModel loadJsonFromFile(String filePath) {
        try {
            File file = new File(filePath);

            // Check if the file exists
            if (!file.exists()) {
                logger.info("File not found: {}", filePath);
                return null;
            }

            // Read the entire file content
            String jsonContent = Files.readString(Path.of(file.getPath()));
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
            return objectMapper.readValue(jsonContent, SourceInputModel.class);
        } catch (Exception e) {
            logger.info("An error occurred while reading the file: {}", e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public static void writeRawMessagesToJsonFile(List<SourceInputModel> rawMessages, Properties props) throws IOException {
        if (!Constants.OUTPUT_FOLDER.exists()) {
            Constants.OUTPUT_FOLDER.mkdirs();
        }

        int rowLimit;
        try {
            String rowLimitStr = props.getProperty(Constants.JSON_OBJJECT_LIMIT, String.valueOf(Constants.DEFAULT_ROW_LIMIT));
            rowLimit = Integer.parseInt(rowLimitStr);
        } catch (NumberFormatException e) {
            logger.error("Invalid row limit value for JSON splitting, using default: {}", Constants.DEFAULT_ROW_LIMIT);
            rowLimit = Constants.DEFAULT_ROW_LIMIT;
        }

        String batchType = props.getProperty(Constants.BATCH_TYPE).toUpperCase();
        String misDate = props.getProperty(Constants.MIS_DATE);
        String runNo = props.getProperty(Constants.RUN_NO);

        String prefix;
        String shortPrefix;
        if ("ISO20022".equals(batchType)) {
            prefix = misDate + "_RUN" + runNo + "_STG_TRANSACTIONS_ENTRY_";
            shortPrefix = "RUN" + runNo + "_STG_TRANSACTIONS_ENTRY_";
        } else if ("NACHA".equals(batchType)) {
            prefix = misDate + "_RUN" + runNo + "_ACH_STG_TRANSACTIONS_ENTRY_";
            shortPrefix = "RUN" + runNo + "_ACH_STG_TRANSACTIONS_ENTRY_";
        } else {
            logger.error("Invalid batchtype: {}", batchType);
            throw new IllegalArgumentException("Invalid batchtype");
        }

        List<String> fileList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.getFactory().setCharacterEscapes(new CustomEscapes());

        if (rawMessages.size() <= rowLimit) {
            // Write to a single file if splitting is not enabled or data is within limit
            String fileName = prefix + "1" + Constants.JSON_EXT;
            File outputFile = new File(Constants.OUTPUT_FOLDER, fileName);
            mapper.writeValue(outputFile, rawMessages);
            fileList.add(shortPrefix + "1");
            logger.info("Successfully wrote raw messages to JSON file: {}", fileName);
        } else {
            // Split data into multiple files
            int fileIndex = 1;
            int startIndex = 0;
            while (startIndex < rawMessages.size()) {
                int endIndex = Math.min(startIndex + rowLimit, rawMessages.size());
                List<SourceInputModel> chunk = rawMessages.subList(startIndex, endIndex);
                String fileName = prefix + fileIndex + Constants.JSON_EXT;
                File outputFile = new File(Constants.OUTPUT_FOLDER, fileName);
                mapper.writeValue(outputFile, chunk);
                fileList.add(shortPrefix + fileIndex);
                logger.info("Successfully wrote raw messages to JSON file: {}", fileName);
                fileIndex++;
                startIndex = endIndex;
            }
            logger.info("Successfully wrote to multiple JSON files with prefix: {}", prefix);
        }

        // Write filename.txt
        File listFile = new File(Constants.OUTPUT_FOLDER, Constants.FILE_NAME_LIST);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(listFile))) {
            for (String fileEntry : fileList) {
                writer.write(fileEntry);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("Error writing filename.txt: {}", e.getMessage());
        }
        logger.info("filename.txt created with list of files.");
    }
}
