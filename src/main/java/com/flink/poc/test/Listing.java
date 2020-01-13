package com.flink.poc.test;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class Listing {

    private String listingIdL;
    private String agentIdL;
    private String buyerAgentIdL;
    private String coListAgentIdL;
    private String coBuyerAgentIdL;
    private String earnestPayableToL;
    private String statusChangeDateL;
    private String inclusionsL;
    private String countyL;
    private String termsOfferedL;
    private String nbrOfAcresL;
    private String coListingMemberUrlL;
    private String listOfficeBoardCodeL;
    private String list207L;
    private Long rowNumL;

    public Listing() {

    }

    public String getListingIdL() {
        return listingIdL;
    }

    public void setListingIdL(String listingIdL) {
        this.listingIdL = listingIdL;
    }

    public String getAgentIdL() {
        return agentIdL;
    }

    public void setAgentIdL(String agentIdL) {
        this.agentIdL = agentIdL;
    }

    public String getBuyerAgentIdL() {
        return buyerAgentIdL;
    }

    public void setBuyerAgentIdL(String buyerAgentIdL) {
        this.buyerAgentIdL = buyerAgentIdL;
    }

    public String getCoListAgentIdL() {
        return coListAgentIdL;
    }

    public void setCoListAgentIdL(String coListAgentIdL) {
        this.coListAgentIdL = coListAgentIdL;
    }

    public String getCoBuyerAgentIdL() {
        return coBuyerAgentIdL;
    }

    public void setCoBuyerAgentIdL(String coBuyerAgentIdL) {
        this.coBuyerAgentIdL = coBuyerAgentIdL;
    }

    public String getEarnestPayableToL() {
        return earnestPayableToL;
    }

    public void setEarnestPayableToL(String earnestPayableToL) {
        this.earnestPayableToL = earnestPayableToL;
    }

    public String getStatusChangeDateL() {
        return statusChangeDateL;
    }

    public void setStatusChangeDateL(String statusChangeDateL) {
        this.statusChangeDateL = statusChangeDateL;
    }

    public String getInclusionsL() {
        return inclusionsL;
    }

    public void setInclusionsL(String inclusionsL) {
        this.inclusionsL = inclusionsL;
    }

    public String getCountyL() {
        return countyL;
    }

    public void setCountyL(String countyL) {
        this.countyL = countyL;
    }

    public String getTermsOfferedL() {
        return termsOfferedL;
    }

    public void setTermsOfferedL(String termsOfferedL) {
        this.termsOfferedL = termsOfferedL;
    }

    public String getNbrOfAcresL() {
        return nbrOfAcresL;
    }

    public void setNbrOfAcresL(String nbrOfAcresL) {
        this.nbrOfAcresL = nbrOfAcresL;
    }

    public String getCoListingMemberUrlL() {
        return coListingMemberUrlL;
    }

    public void setCoListingMemberUrlL(String coListingMemberUrlL) {
        this.coListingMemberUrlL = coListingMemberUrlL;
    }

    public String getListOfficeBoardCodeL() {
        return listOfficeBoardCodeL;
    }

    public void setListOfficeBoardCodeL(String listOfficeBoardCodeL) {
        this.listOfficeBoardCodeL = listOfficeBoardCodeL;
    }

    public String getList207L() {
        return list207L;
    }

    public void setList207L(String list207L) {
        this.list207L = list207L;
    }

    public Long getRowNumL() {
        return rowNumL;
    }

    public void setRowNumL(Long rowNumL) {
        this.rowNumL = rowNumL;
    }

    @Override
    public String toString() {
        return "Listing{" +
                "listingIdL='" + listingIdL + '\'' +
                ", agentIdL='" + agentIdL + '\'' +
                ", buyerAgentIdL='" + buyerAgentIdL + '\'' +
                ", coListAgentIdL='" + coListAgentIdL + '\'' +
                ", coBuyerAgentIdL='" + coBuyerAgentIdL + '\'' +
                ", earnestPayableToL='" + earnestPayableToL + '\'' +
                ", statusChangeDateL='" + statusChangeDateL + '\'' +
                ", inclusionsL='" + inclusionsL + '\'' +
                ", countyL='" + countyL + '\'' +
                ", termsOfferedL='" + termsOfferedL + '\'' +
                ", nbrOfAcresL='" + nbrOfAcresL + '\'' +
                ", coListingMemberUrlL='" + coListingMemberUrlL + '\'' +
                ", listOfficeBoardCodeL='" + listOfficeBoardCodeL + '\'' +
                ", list207L='" + list207L + '\'' +
                ", rowNumL=" + rowNumL +
                '}';
    }
}
