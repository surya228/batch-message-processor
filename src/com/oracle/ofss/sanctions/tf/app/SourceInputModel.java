package com.oracle.ofss.sanctions.tf.app;



import java.util.HashMap;

public class SourceInputModel {
	
    private String rawMessage;
    private String businessDomainCode;
    private String jurisdictionCode;
    private String messageDirection;
    private HashMap<String, String> additionalData;
    
	public SourceInputModel() {
		super();
	}

	public SourceInputModel(String rawMessage, String businessDomainCode, String jurisdictionCode, String messageDirection, HashMap<String, String> additionalData) {
		super();
		this.rawMessage = rawMessage;
		this.businessDomainCode = businessDomainCode;
		this.jurisdictionCode = jurisdictionCode;
		this.messageDirection = messageDirection;
		this.additionalData = additionalData;
	}
	
	public String getRawMessage() {
		return rawMessage;
	}
	public void setRawMessage(String rawMessage) {
		this.rawMessage = rawMessage;
	}
	public String getBusinessDomainCode() {
		return businessDomainCode;
	}
	public void setBusinessDomainCode(String businessDomainCode) {
		this.businessDomainCode = businessDomainCode;
	}
	public String getJurisdictionCode() {
		return jurisdictionCode;
	}
	public void setJurisdictionCode(String jurisdictionCode) {
		this.jurisdictionCode = jurisdictionCode;
	}
	public String getMessageDirection() {
		return messageDirection;
	}
	public void setMessageDirection(String messageDirection) {
		this.messageDirection = messageDirection;
	}
	public HashMap<String, String> getAdditionalData() {
		return additionalData;
	}
	public void setAdditionalData(HashMap<String, String> additionalData) {
		this.additionalData = additionalData;
	}

}
