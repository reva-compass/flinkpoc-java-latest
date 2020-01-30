package com.flink.poc.seattle;

public class OpenHouse {

    private String pkOH;
    private String listingKeyOH;
    private String dataOH;

    public String getPkOH() {
        return pkOH;
    }

    public void setPkOH(String pkOH) {
        this.pkOH = pkOH;
    }

    public String getListingKeyOH() {
        return listingKeyOH;
    }

    public void setListingKeyOH(String listingKeyOH) {
        this.listingKeyOH = listingKeyOH;
    }

    public String getDataOH() {
        return dataOH;
    }

    public void setDataOH(String dataOH) {
        this.dataOH = dataOH;
    }

    @Override
    public String toString() {
        return "OpenHouse{" +
                "pkOH='" + pkOH + '\'' +
                ", listingKeyOH='" + listingKeyOH + '\'' +
                ", dataOH='" + dataOH + '\'' +
                '}';
    }
}
