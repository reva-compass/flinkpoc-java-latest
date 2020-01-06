package com.flink.poc.crmls;

public class Office {

    private String ucPK;
    private String ucUpdateTS;
    private String ucVersion;
    private String ucRowType;
    private String ucType;
    private String data;

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

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Office{" +
                "ucPK='" + ucPK + '\'' +
                ", ucUpdateTS='" + ucUpdateTS + '\'' +
                ", ucVersion='" + ucVersion + '\'' +
                ", ucRowType='" + ucRowType + '\'' +
                ", ucType='" + ucType + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
