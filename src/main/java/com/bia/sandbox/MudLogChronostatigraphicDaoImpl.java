package com.bia.sandbox;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ChronostratigraphyStruct;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MudLogChronostatigraphicDaoImpl implements MudLogChronostatigraphicDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_chronostatigraphic";

    public MudLogChronostatigraphicDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), MudLogChronostatigraphicDaoImpl.TABLE_NAME);
    }

    @Override
    public void save(ChronostratigraphyStruct chronostratigraphyStruct, CsGeologyInterval csGeologyInterval) {
        TableRow data = new TableRow();
        data.set("uid_geologyinterval", csGeologyInterval.getUid());
        if(chronostratigraphyStruct.getKind() != null){
            data.set("kind", chronostratigraphyStruct.getKind().value());
        }
        data.set("value", chronostratigraphyStruct.getValue());

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
