package com.flink.poc.seattle;

public class Office {

    //   PRIMARY_KEY = 'OfficeMLSID'
    private String pkO;
    private String dataO;

    public String getPkO() {
        return pkO;
    }

    public void setPkO(String pkO) {
        this.pkO = pkO;
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
                "pk='" + pkO + '\'' +
                ", dataO='" + dataO + '\'' +
                '}';
    }
}
