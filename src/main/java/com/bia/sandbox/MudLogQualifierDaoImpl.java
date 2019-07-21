package com.bia.sandbox;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsQualifier;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MudLogQualifierDaoImpl implements MudLogQualifierDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_qualifier";

    public MudLogQualifierDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), MudLogQualifierDaoImpl.TABLE_NAME);
    }

    @Override
    public void save(CsQualifier csQualifier, CsLithology csLithology) {

        TableRow data = new TableRow();
        data.set("uid", csQualifier.getUid());
        data.set("uid_lithology", csLithology.getUid());
        data.set("type", csQualifier.getType());
        data.set("abundance", csQualifier.getAbundance().getValue());
        data.set("abundance_uom", csQualifier.getAbundance().getUom());
        data.set("description", csQualifier.getDescription());

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
