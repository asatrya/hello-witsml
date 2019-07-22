package com.bia.sandbox.mudlog;

import com.bia.sandbox.config.ConnectionFactory;
import com.bia.sandbox.config.GlobalOptions;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.LithostratigraphyStruct;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LithostatigraphicDaoImpl implements LithostatigraphicDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_lithostatigraphic";

    public LithostatigraphicDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), LithostatigraphicDaoImpl.TABLE_NAME);
    }

    @Override
    public void save(LithostratigraphyStruct lithostratigraphyStruct, CsGeologyInterval csGeologyInterval) {
        TableRow data = new TableRow();
        data.set("uid_geologyinterval", csGeologyInterval.getUid());
        if(lithostratigraphyStruct.getKind() != null){
            data.set("kind", lithostratigraphyStruct.getKind().value());
        }
        data.set("value", lithostratigraphyStruct.getValue());

        InsertAllRequest request = null;
        try{
            request = InsertAllRequest.newBuilder(this.tableId).addRow(data).build();
            InsertAllResponse response = this.bigQuery.insertAll(request);

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    System.out.println(entry.toString());
                }
            }
        }catch (IllegalArgumentException ex){
            System.out.println(request);
            ex.printStackTrace();
        }
    }
}
