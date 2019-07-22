package com.bia.sandbox.mudlog;

import com.bia.sandbox.config.ConnectionFactory;
import com.bia.sandbox.config.GlobalOptions;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsQualifier;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class QualifierDaoImpl implements QualifierDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_qualifier";

    public QualifierDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), QualifierDaoImpl.TABLE_NAME);
    }

    @Override
    public void save(CsQualifier csQualifier, CsLithology csLithology) {

        TableRow data = new TableRow();
        data.set("uid", csQualifier.getUid());
        data.set("uid_lithology", csLithology.getUid());
        if(csQualifier.getType() != null){
            data.set("type", csQualifier.getType());
        }
        if(csQualifier.getAbundance() != null){
            data.set("abundance", csQualifier.getAbundance().getValue());
        }
        if(csQualifier.getAbundance() != null){
            data.set("abundance_uom", csQualifier.getAbundance().getUom());
        }
        if(csQualifier.getDescription() != null){
            data.set("description", csQualifier.getDescription());
        }

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
