package com.bia.sandbox;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsMudLogParameter;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MudLogParameterDaoImpl implements MudLogParameterDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_parameters";

    public MudLogParameterDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), MudLogParameterDaoImpl.TABLE_NAME);
    }

    public void save(CsMudLogParameter csMudLogParameter, ObjMudLog objMudLog) {
        TableRow data = new TableRow();
        data.set("uid", csMudLogParameter.getUid());
        data.set("uid_mudLogs", objMudLog.getUid());
        if(csMudLogParameter.getType() != null){
            data.set("type", csMudLogParameter.getType().value());
        }
        if(csMudLogParameter.getDTime() != null){
            data.set("dTime", BQUtil.convertToBQDate(csMudLogParameter.getDTime()));
        }
        if(csMudLogParameter.getMdTop() != null){
            data.set("mdTop", csMudLogParameter.getMdTop().getValue());
        }
        if(csMudLogParameter.getMdTop() != null){
            data.set("mdTop_uom", csMudLogParameter.getMdTop().getUom().value());
        }
        if(csMudLogParameter.getMdBottom() != null){
            data.set("mdBottom", csMudLogParameter.getMdBottom().getValue());
        }
        if(csMudLogParameter.getMdBottom() != null){
            data.set("mdBottom_uom", csMudLogParameter.getMdBottom().getUom().value());
        }
        if(csMudLogParameter.getText() != null){
            data.set("text", csMudLogParameter.getText());
        }
        if(csMudLogParameter.getCommonTime() != null){
            if(csMudLogParameter.getCommonTime().getDTimCreation() != null){
                data.set("commonTime_dTimCreation", BQUtil.convertToBQDate(csMudLogParameter.getCommonTime().getDTimCreation()));
            }
            if(csMudLogParameter.getCommonTime().getDTimLastChange() != null){
                data.set("commonTime_dTimLastChange", BQUtil.convertToBQDate(csMudLogParameter.getCommonTime().getDTimLastChange()));
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
