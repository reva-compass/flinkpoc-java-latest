package com.flink.poc.crmls;

public class Agent {

    private String ucPKA;
    private String ucUpdateTSA;
    private String ucVersionA;
    private String ucRowTypeA;
    private String ucTypeA;
    private String dataA;

    public String getUcPKA() {
        return ucPKA;
    }

    public void setUcPKA(String ucPKA) {
        this.ucPKA = ucPKA;
    }

    public String getUcUpdateTSA() {
        return ucUpdateTSA;
    }

    public void setUcUpdateTSA(String ucUpdateTSA) {
        this.ucUpdateTSA = ucUpdateTSA;
    }

    public String getUcVersionA() {
        return ucVersionA;
    }

    public void setUcVersionA(String ucVersionA) {
        this.ucVersionA = ucVersionA;
    }

    public String getUcRowTypeA() {
        return ucRowTypeA;
    }

    public void setUcRowTypeA(String ucRowTypeA) {
        this.ucRowTypeA = ucRowTypeA;
    }

    public String getUcTypeA() {
        return ucTypeA;
    }

    public void setUcTypeA(String ucTypeA) {
        this.ucTypeA = ucTypeA;
    }

    public String getDataA() {
        return dataA;
    }

    public void setDataA(String dataA) {
        this.dataA = dataA;
    }

    @Override
    public String toString() {
        return "Agent{" +
                "ucPKA='" + ucPKA + '\'' +
                ", ucUpdateTSA='" + ucUpdateTSA + '\'' +
                ", ucVersionA='" + ucVersionA + '\'' +
                ", ucRowTypeA='" + ucRowTypeA + '\'' +
                ", ucTypeA='" + ucTypeA + '\'' +
                ", dataA='" + dataA + '\'' +
                '}';
    }
}
