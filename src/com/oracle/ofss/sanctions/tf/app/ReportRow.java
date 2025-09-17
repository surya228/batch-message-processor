package com.oracle.ofss.sanctions.tf.app;

public class ReportRow {
    public int seqNo;
    public String ruleName;
    public String message;
    public String tag;
    public String sourceInput;
    public String targetInput;
    public String targetColumn;
    public String watchlist;
    public String nUid;
    public long transactionToken;
    public String runSkey;
    public int matchCount;
    public String feedbackStatus;
    public int specificMatches;
    public String feedback;
    public String testStatus;
    public String comments;
    public String messageKey;
    public boolean isColumnMismatch;

public ReportRow(int seqNo, String ruleName, String message, String tag, String sourceInput, String targetInput,
                     String targetColumn, String watchlist, String nUid, long transactionToken, String runSkey,
                     int matchCount, String feedbackStatus, int specificMatches, String feedback,
                     String testStatus, String comments, String messageKey, boolean isColumnMismatch) {
        this.seqNo = seqNo;
        this.ruleName = ruleName;
        this.message = message;
        this.tag = tag;
        this.sourceInput = sourceInput;
        this.targetInput = targetInput;
        this.targetColumn = targetColumn;
        this.watchlist = watchlist;
        this.nUid = nUid;
        this.transactionToken = transactionToken;
        this.runSkey = runSkey;
        this.matchCount = matchCount;
        this.feedbackStatus = feedbackStatus;
        this.specificMatches = specificMatches;
        this.feedback = feedback;
        this.testStatus = testStatus;
        this.comments = comments;
        this.messageKey = messageKey;
        this.isColumnMismatch = isColumnMismatch;
    }
}
