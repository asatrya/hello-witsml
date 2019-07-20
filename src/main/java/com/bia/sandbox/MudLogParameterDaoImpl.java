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
        data.set("type", csMudLogParameter.getType().value());
        data.set("dTime", BQUtil.convertToBQDate(csMudLogParameter.getDTime()));
        data.set("mdTop", csMudLogParameter.getMdTop().getValue());
        data.set("mdTop_uom", csMudLogParameter.getMdTop().getUom().value());
        data.set("mdBottom", csMudLogParameter.getMdBottom().getValue());
        data.set("mdBottom_uom", csMudLogParameter.getMdBottom().getUom().value());
        data.set("text", csMudLogParameter.getText());
        data.set("commonTime_dTimCreation", BQUtil.convertToBQDate(csMudLogParameter.getCommonTime().getDTimCreation()));
        data.set("commonTime_dTimLastChange", BQUtil.convertToBQDate(csMudLogParameter.getCommonTime().getDTimLastChange()));

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
