package com.flink.poc.crmls;

public class Office {

    private String ucPKO;
    private String ucUpdateTSO;
    private String ucVersionO;
    private String ucRowTypeO;
    private String ucTypeO;
    private String dataO;

    public String getUcPKO() {
        return ucPKO;
    }

    public void setUcPKO(String ucPKO) {
        this.ucPKO = ucPKO;
    }

    public String getUcUpdateTSO() {
        return ucUpdateTSO;
    }

    public void setUcUpdateTSO(String ucUpdateTSO) {
        this.ucUpdateTSO = ucUpdateTSO;
    }

    public String getUcVersionO() {
        return ucVersionO;
    }

    public void setUcVersionO(String ucVersionO) {
        this.ucVersionO = ucVersionO;
    }

    public String getUcRowTypeO() {
        return ucRowTypeO;
    }

    public void setUcRowTypeO(String ucRowTypeO) {
        this.ucRowTypeO = ucRowTypeO;
    }

    public String getUcTypeO() {
        return ucTypeO;
    }

    public void setUcTypeO(String ucTypeO) {
        this.ucTypeO = ucTypeO;
    }

    public String getDataO() {
        return dataO;
    }

    public void setDataO(String dataO) {
        this.dataO = dataO;
    }

    @Override
    public String toString() {
        return "Office{" +
                "ucPKO='" + ucPKO + '\'' +
                ", ucUpdateTSO='" + ucUpdateTSO + '\'' +
                ", ucVersionO='" + ucVersionO + '\'' +
                ", ucRowTypeO='" + ucRowTypeO + '\'' +
                ", ucTypeO='" + ucTypeO + '\'' +
                ", dataO='" + dataO + '\'' +
                '}';
    }
}
