package com.flink.poc.test;

public class Agent {

    private String agentIdA;
    private String cityA;
    private String officeIdA;
    private String emailA;
    private String renegotiationExpA;
    private String nrdsidA;
    private String mlsStatusA;
    private String agentTimestampA;

    public Agent() {

    }

    public String getAgentIdA() {
        return agentIdA;
    }

    public void setAgentIdA(String agentIdA) {
        this.agentIdA = agentIdA;
    }

    public String getCityA() {
        return cityA;
    }

    public void setCityA(String cityA) {
        this.cityA = cityA;
    }

    public String getOfficeIdA() {
        return officeIdA;
    }

    public void setOfficeIdA(String officeIdA) {
        this.officeIdA = officeIdA;
    }

    public String getEmailA() {
        return emailA;
    }

    public void setEmailA(String emailA) {
        this.emailA = emailA;
    }

    public String getRenegotiationExpA() {
        return renegotiationExpA;
    }

    public void setRenegotiationExpA(String renegotiationExpA) {
        this.renegotiationExpA = renegotiationExpA;
    }

    public String getNrdsidA() {
        return nrdsidA;
    }

    public void setNrdsidA(String nrdsidA) {
        this.nrdsidA = nrdsidA;
    }

    public String getMlsStatusA() {
        return mlsStatusA;
    }

    public void setMlsStatusA(String mlsStatusA) {
        this.mlsStatusA = mlsStatusA;
    }

    public String getAgentTimestampA() {
        return agentTimestampA;
    }

    public void setAgentTimestampA(String agentTimestampA) {
        this.agentTimestampA = agentTimestampA;
    }

    @Override
    public String toString() {
        return "Agent{" +
                "agentIdA='" + agentIdA + '\'' +
                ", cityA='" + cityA + '\'' +
                ", officeIdA='" + officeIdA + '\'' +
                ", emailA='" + emailA + '\'' +
                ", renegotiationExpA='" + renegotiationExpA + '\'' +
                ", nrdsidA='" + nrdsidA + '\'' +
                ", mlsStatusA='" + mlsStatusA + '\'' +
                ", agentTimestampA='" + agentTimestampA + '\'' +
                '}';
    }
}
