package com.bia.sandbox;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MudLogLithologyDaoImpl implements MudLogLithologyDao{

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_lithology";

    public MudLogLithologyDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), MudLogLithologyDaoImpl.TABLE_NAME);
    }

    @Override
    public void save(CsLithology csLithology, CsGeologyInterval csGeologyInterval) {
        TableRow data = new TableRow();
        data.set("uid", csLithology.getUid());
        data.set("uid_geologyInterval", csGeologyInterval.getUid());
        data.set("type", csLithology.getType());
        data.set("description", csLithology.getDescription());
        data.set("color", csLithology.getColor());
        data.set("texture", csLithology.getTexture());
        data.set("hardness", csLithology.getHardness());
        data.set("sizeGrain", csLithology.getSizeGrain());
        data.set("roundness", csLithology.getRoundness());
        data.set("sorting", csLithology.getSorting());
        data.set("matrixCement", csLithology.getMatrixCement());
        data.set("porosityVisible", csLithology.getPorosityVisible());
        data.set("permeability", csLithology.getPermeability());
        data.set("densShale", csLithology.getDensShale().getValue());
        data.set("densShale_uom", csLithology.getDensShale().getUom());

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
