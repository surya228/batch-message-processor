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
    public long osTransactionToken;
    public String osRunSkey;
    public int osMatchCount;
    public String osFeedbackStatus;
    public int osSpecificMatches;
    public String osFeedback;
    public String osTestStatus;
    public String osComments;

    public ReportRow(int seqNo, String ruleName, String message, String tag, String sourceInput, String targetInput,
                     String targetColumn, String watchlist, String nUid, long osTransactionToken, String osRunSkey,
                     int osMatchCount, String osFeedbackStatus, int osSpecificMatches, String osFeedback,
                     String osTestStatus, String osComments) {
        this.seqNo = seqNo;
        this.ruleName = ruleName;
        this.message = message;
        this.tag = tag;
        this.sourceInput = sourceInput;
        this.targetInput = targetInput;
        this.targetColumn = targetColumn;
        this.watchlist = watchlist;
        this.nUid = nUid;
        this.osTransactionToken = osTransactionToken;
        this.osRunSkey = osRunSkey;
        this.osMatchCount = osMatchCount;
        this.osFeedbackStatus = osFeedbackStatus;
        this.osSpecificMatches = osSpecificMatches;
        this.osFeedback = osFeedback;
        this.osTestStatus = osTestStatus;
        this.osComments = osComments;
    }
}
