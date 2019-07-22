package com.bia.sandbox.mudlog;

import com.bia.sandbox.util.BQUtil;
import com.bia.sandbox.config.ConnectionFactory;
import com.bia.sandbox.config.GlobalOptions;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MudLogDaoImpl implements MudLogDao {

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
        if(mudLogObj.getUidWell() != null){
            data.set("uidWell", mudLogObj.getUidWell());
        }
        if(mudLogObj.getUidWellbore() != null){
            data.set("uidWellbore", mudLogObj.getUidWellbore());
        }
        if(mudLogObj.getNameWell() != null){
            data.set("nameWell", mudLogObj.getNameWell());
        }
        if(mudLogObj.getNameWellbore() != null){
            data.set("nameWellbore", mudLogObj.getNameWellbore());
        }
        if(mudLogObj.getName() != null){
            data.set("name", mudLogObj.getName());
        }
        if(mudLogObj.getDTim() != null){
            data.set("dTim", BQUtil.convertToBQDate(mudLogObj.getDTim()));
        }
        if(mudLogObj.getMudLogCompany() != null){
            data.set("mudLogCompany", mudLogObj.getMudLogCompany());
        }
        if(mudLogObj.getMudLogEngineers() != null){
            data.set("mudLogEngineers", mudLogObj.getMudLogEngineers());
        }
        if(mudLogObj.getStartMd() != null){
            data.set("startMd", mudLogObj.getStartMd().getValue());
        }
        if(mudLogObj.getStartMd() != null){
            data.set("startMd_uom", mudLogObj.getStartMd().getUom().value());
        }
        if(mudLogObj.getEndMd() != null){
            data.set("endMd", mudLogObj.getEndMd().getValue());
        }
        if(mudLogObj.getEndMd() != null){
            data.set("endMd_uom", mudLogObj.getEndMd().getUom().value());
        }
        if(mudLogObj.getCommonData() != null){
            if(mudLogObj.getCommonData().getDTimCreation() != null){
                data.set("commonData_dTimCreation", BQUtil.convertToBQDate(mudLogObj.getCommonData().getDTimCreation()));
            }
            if(mudLogObj.getCommonData().getDTimLastChange() != null){
                data.set("commonData_dTimLastChange", BQUtil.convertToBQDate(mudLogObj.getCommonData().getDTimLastChange()));
            }
            if(mudLogObj.getCommonData().getItemState() != null){
                data.set("commonData_itemState", mudLogObj.getCommonData().getItemState().value());
            }
            if(mudLogObj.getCommonData().getDefaultDatum() != null){
                data.set("commonData_defaultDatum", mudLogObj.getCommonData().getDefaultDatum().getValue());
            }
        }

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