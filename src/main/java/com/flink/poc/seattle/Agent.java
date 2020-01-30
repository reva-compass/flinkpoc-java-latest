package com.flink.poc.seattle;

public class Agent {

//   PRIMARY_KEY = 'MemberMLSID'
   private String pkA;
    private String dataA;

    public String getPkA() {
        return pkA;
    }

    public void setPkA(String pkA) {
        this.pkA = pkA;
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
                "memberMLSID='" + pkA + '\'' +
                ", dataA='" + dataA + '\'' +
                '}';
    }
}
