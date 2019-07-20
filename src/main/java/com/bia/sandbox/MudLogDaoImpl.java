package com.bia.sandbox;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class MudLogDaoImpl implements MudLogDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs";

    public MudLogDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), MudLogDaoImpl.TABLE_NAME);
    }

    public void save(ObjMudLog mudLogObj) {
        TableRow data = new TableRow();
        data.set("uid", mudLogObj.getUid());
        data.set("uidWell", mudLogObj.getUidWell());
        data.set("uidWellbore", mudLogObj.getUidWellbore());
        data.set("nameWell", mudLogObj.getNameWell());
        data.set("nameWellbore", mudLogObj.getNameWellbore());
        data.set("name", mudLogObj.getName());
        data.set("dTim", BQUtil.convertToBQDate(mudLogObj.getDTim()));
        data.set("mudLogCompany", mudLogObj.getMudLogCompany());
        data.set("mudLogEngineers", mudLogObj.getMudLogEngineers());
        data.set("startMd", mudLogObj.getStartMd().getValue());
        data.set("startMd_uom", mudLogObj.getStartMd().getUom().value());
        data.set("endMd", mudLogObj.getEndMd().getValue());
        data.set("endMd_uom", mudLogObj.getEndMd().getUom().value());
        data.set("commonData_dTimCreation", BQUtil.convertToBQDate(mudLogObj.getCommonData().getDTimCreation()));
        data.set("commonData_dTimLastChange", BQUtil.convertToBQDate(mudLogObj.getCommonData().getDTimLastChange()));
        data.set("commonData_itemState", mudLogObj.getCommonData().getItemState().value());
        data.set("commonData_defaultDatum", mudLogObj.getCommonData().getDefaultDatum().getValue());

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