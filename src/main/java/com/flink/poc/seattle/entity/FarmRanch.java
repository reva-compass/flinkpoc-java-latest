package com.flink.poc.seattle.entity;

public class FarmRanch {

    // LISTING_PRIMARY_KEY = 'LN'
    private String pkL;
    // agent keys: 'LAG', 'CLA', 'SAG', 'SCA'
    private String listingAgent;
    private String CLA;
    private String SAG;
    private String SCA;
    // office keys: ['LO', 'COLO', 'SO', 'SCO']
    private String LO;
    private String COLO;
    private String SO;
    private String SCO;
    private String dataL;

    public String getPkL() {
        return pkL;
    }

    public void setPkL(String pkL) {
        this.pkL = pkL;
    }

    public String getListingAgent() {
        return listingAgent;
    }

    public void setListingAgent(String listingAgent) {
        this.listingAgent = listingAgent;
    }

    public String getCLA() {
        return CLA;
    }

    public void setCLA(String CLA) {
        this.CLA = CLA;
    }

    public String getSAG() {
        return SAG;
    }

    public void setSAG(String SAG) {
        this.SAG = SAG;
    }

    public String getSCA() {
        return SCA;
    }

    public void setSCA(String SCA) {
        this.SCA = SCA;
    }

    public String getLO() {
        return LO;
    }

    public void setLO(String LO) {
        this.LO = LO;
    }

    public String getCOLO() {
        return COLO;
    }

    public void setCOLO(String COLO) {
        this.COLO = COLO;
    }

    public String getSO() {
        return SO;
    }

    public void setSO(String SO) {
        this.SO = SO;
    }

    public String getSCO() {
        return SCO;
    }

    public void setSCO(String SCO) {
        this.SCO = SCO;
    }

    public String getDataL() {
        return dataL;
    }

    public void setDataL(String dataL) {
        this.dataL = dataL;
    }

    @Override
    public String toString() {
        return "FarmRanch{" +
                "pkL='" + pkL + '\'' +
                ", listingAgent='" + listingAgent + '\'' +
                ", CLA='" + CLA + '\'' +
                ", SAG='" + SAG + '\'' +
                ", SCA='" + SCA + '\'' +
                ", LO='" + LO + '\'' +
                ", COLO='" + COLO + '\'' +
                ", SO='" + SO + '\'' +
                ", SCO='" + SCO + '\'' +
                ", dataL='" + dataL + '\'' +
                '}';
    }
}
