package com.flink.poc.crmls;

public class OpenHouse {

    private String ucPKOH;
    private String ucUpdateTSOH;
    private String ucVersionOH;
    private String ucRowTypeOH;
    private String ucTypeOH;
    private String listingKeyOH;
    private String dataOH;

    public String getUcPKOH() {
        return ucPKOH;
    }

    public void setUcPKOH(String ucPKOH) {
        this.ucPKOH = ucPKOH;
    }

    public String getUcUpdateTSOH() {
        return ucUpdateTSOH;
    }

    public void setUcUpdateTSOH(String ucUpdateTSOH) {
        this.ucUpdateTSOH = ucUpdateTSOH;
    }

    public String getUcVersionOH() {
        return ucVersionOH;
    }

    public void setUcVersionOH(String ucVersionOH) {
        this.ucVersionOH = ucVersionOH;
    }

    public String getUcRowTypeOH() {
        return ucRowTypeOH;
    }

    public void setUcRowTypeOH(String ucRowTypeOH) {
        this.ucRowTypeOH = ucRowTypeOH;
    }

    public String getUcTypeOH() {
        return ucTypeOH;
    }

    public void setUcTypeOH(String ucTypeOH) {
        this.ucTypeOH = ucTypeOH;
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
                "ucPKOH='" + ucPKOH + '\'' +
                ", ucUpdateTSOH='" + ucUpdateTSOH + '\'' +
                ", ucVersionOH='" + ucVersionOH + '\'' +
                ", ucRowTypeOH='" + ucRowTypeOH + '\'' +
                ", ucTypeOH='" + ucTypeOH + '\'' +
                ", listingKeyOH='" + listingKeyOH + '\'' +
                ", dataOH='" + dataOH + '\'' +
                '}';
    }
}
