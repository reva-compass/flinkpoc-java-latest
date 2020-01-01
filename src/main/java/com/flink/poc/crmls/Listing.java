package com.flink.poc.crmls;

public class Listing {

    private String ucPK;
    private String ucUpdateTS;
    private String ucVersion;
    private String ucRowType;
    private String ucType;
    private String listingKey;
    private String listAgentKey;
    private String buyerAgentKey;
    private String coListAgentKey;
    private String coBuyerAgentKey;
    private String listOfficeKey;
    private String buyerOfficeKey;
    private String coListOfficeKey;
    private String coBuyerOfficeKey;
    private String data;
    //private Long rowNum;

    public Listing() {

    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getUcPK() {
        return ucPK;
    }

    public void setUcPK(String ucPK) {
        this.ucPK = ucPK;
    }

    public String getUcUpdateTS() {
        return ucUpdateTS;
    }

    public void setUcUpdateTS(String ucUpdateTS) {
        this.ucUpdateTS = ucUpdateTS;
    }

    public String getUcVersion() {
        return ucVersion;
    }

    public void setUcVersion(String ucVersion) {
        this.ucVersion = ucVersion;
    }

    public String getUcRowType() {
        return ucRowType;
    }

    public void setUcRowType(String ucRowType) {
        this.ucRowType = ucRowType;
    }

    public String getUcType() {
        return ucType;
    }

    public void setUcType(String ucType) {
        this.ucType = ucType;
    }

    public String getListingKey() {
        return listingKey;
    }

    public void setListingKey(String listingKey) {
        this.listingKey = listingKey;
    }

    public String getListAgentKey() {
        return listAgentKey;
    }

    public void setListAgentKey(String listAgentKey) {
        this.listAgentKey = listAgentKey;
    }

    public String getBuyerAgentKey() {
        return buyerAgentKey;
    }

    public void setBuyerAgentKey(String buyerAgentKey) {
        this.buyerAgentKey = buyerAgentKey;
    }

    public String getCoListAgentKey() {
        return coListAgentKey;
    }

    public void setCoListAgentKey(String coListAgentKey) {
        this.coListAgentKey = coListAgentKey;
    }

    public String getCoBuyerAgentKey() {
        return coBuyerAgentKey;
    }

    public void setCoBuyerAgentKey(String coBuyerAgentKey) {
        this.coBuyerAgentKey = coBuyerAgentKey;
    }

    public String getListOfficeKey() {
        return listOfficeKey;
    }

    public void setListOfficeKey(String listOfficeKey) {
        this.listOfficeKey = listOfficeKey;
    }

    public String getBuyerOfficeKey() {
        return buyerOfficeKey;
    }

    public void setBuyerOfficeKey(String buyerOfficeKey) {
        this.buyerOfficeKey = buyerOfficeKey;
    }

    public String getCoListOfficeKey() {
        return coListOfficeKey;
    }

    public void setCoListOfficeKey(String coListOfficeKey) {
        this.coListOfficeKey = coListOfficeKey;
    }

    public String getCoBuyerOfficeKey() {
        return coBuyerOfficeKey;
    }

    public void setCoBuyerOfficeKey(String coBuyerOfficeKey) {
        this.coBuyerOfficeKey = coBuyerOfficeKey;
    }

    @Override
    public String toString() {
        return "Listing{" +
                "ucPK='" + ucPK + '\'' +
                ", ucUpdateTS='" + ucUpdateTS + '\'' +
                ", ucVersion='" + ucVersion + '\'' +
                ", ucRowType='" + ucRowType + '\'' +
                ", ucType='" + ucType + '\'' +
                ", listingKey='" + listingKey + '\'' +
                ", listAgentKey='" + listAgentKey + '\'' +
                ", buyerAgentKey='" + buyerAgentKey + '\'' +
                ", coListAgentKey='" + coListAgentKey + '\'' +
                ", coBuyerAgentKey='" + coBuyerAgentKey + '\'' +
                ", listOfficeKey='" + listOfficeKey + '\'' +
                ", buyerOfficeKey='" + buyerOfficeKey + '\'' +
                ", coListOfficeKey='" + coListOfficeKey + '\'' +
                ", coBuyerOfficeKey='" + coBuyerOfficeKey + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
