package com.bia.sandbox;

import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;

import java.util.List;
import java.util.Map;

class MudLogBQ extends BaseModelBQ {

    private String uidWell;
    private String uidWellbore;
    private String nameWell;
    private String nameWellbore;
    private String name;
    private Boolean objectGrowing;
    private DateTime dTim;
    private String mudLogCompany;
    private String mudLogEngineers;
    private Float startMd;
    private String startMd_uom;
    private Float endMd;
    private String endMd_uom;
    private DateTime commonData_dTimCreation;
    private DateTime commonData_dTimLastChange;
    private String commonData_itemState;
    private String commonData_defaultDatum;

    public MudLogBQ(BigQuery bigQuery, String dataSetName, String tableName) {
        super(bigQuery, dataSetName, tableName);
    }

    public void setUidWell(String uidWell) {
        this.uidWell = uidWell;
    }

    public void setUidWellbore(String uidWellbore) {
        this.uidWellbore = uidWellbore;
    }

    public void setNameWell(String nameWell) {
        this.nameWell = nameWell;
    }

    public void setNameWellbore(String nameWellbore) {
        this.nameWellbore = nameWellbore;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setObjectGrowing(Boolean objectGrowing) {
        this.objectGrowing = objectGrowing;
    }

    public void setdTim(DateTime dTim) {
        this.dTim = dTim;
    }

    public void setMudLogCompany(String mudLogCompany) {
        this.mudLogCompany = mudLogCompany;
    }

    public void setMudLogEngineers(String mudLogEngineers) {
        this.mudLogEngineers = mudLogEngineers;
    }

    public void setStartMd(Float startMd) {
        this.startMd = startMd;
    }

    public void setStartMd_uom(String startMd_uom) {
        this.startMd_uom = startMd_uom;
    }

    public void setEndMd(Float endMd) {
        this.endMd = endMd;
    }

    public void setEndMd_uom(String endMd_uom) {
        this.endMd_uom = endMd_uom;
    }

    public void setCommonData_dTimCreation(DateTime commonData_dTimCreation) {
        this.commonData_dTimCreation = commonData_dTimCreation;
    }

    public void setCommonData_dTimLastChange(DateTime commonData_dTimLastChange) {
        this.commonData_dTimLastChange = commonData_dTimLastChange;
    }

    public void setCommonData_itemState(String commonData_itemState) {
        this.commonData_itemState = commonData_itemState;
    }

    public void setCommonData_defaultDatum(String commonData_defaultDatum) {
        this.commonData_defaultDatum = commonData_defaultDatum;
    }

    public void save() {
        TableRow data = new TableRow();
        data.set("uid", this.uid);
        data.set("uidWell", this.uidWell);
        data.set("uidWellbore", this.uidWellbore);
        data.set("nameWell", this.nameWell);
        data.set("nameWellbore", this.nameWellbore);
        data.set("name", this.name);
        data.set("dTim", this.dTim);
        data.set("mudLogCompany", this.mudLogCompany);
        data.set("mudLogEngineers", this.mudLogEngineers);
        data.set("startMd", this.startMd);
        data.set("startMd_uom", this.startMd_uom);
        data.set("endMd", this.endMd);
        data.set("endMd_uom", this.endMd_uom);
        data.set("commonData_dTimCreation", this.commonData_dTimCreation);
        data.set("commonData_dTimLastChange", this.commonData_dTimLastChange);
        data.set("commonData_itemState", this.commonData_itemState);
        data.set("commonData_defaultDatum", this.commonData_defaultDatum);

        InsertAllResponse response =
                this.bigQuery.insertAll(
                        InsertAllRequest.newBuilder(this.tableId)
                                .addRow(data)
                                .build());
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                System.out.println(entry.toString());
            }
        }
    }
}