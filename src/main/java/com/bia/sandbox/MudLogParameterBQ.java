package com.bia.sandbox;

import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;

import java.util.List;
import java.util.Map;

public class MudLogParameterBQ extends BaseModelBQ {

    private String uid;
    private String uid_mudLogs;
    private String type;
    private DateTime dTime;
    private Float mdTop;
    private String mdTop_uom;
    private Float mdBottom;
    private String mdBottom_uom;
    private String text;
    private DateTime commonTime_dTimCreation;
    private DateTime commonTime_dTimLastChange;

    public String getUid() {
        return uid;
    }

    public MudLogParameterBQ(BigQuery bigQuery, String dataSetName, String tableName) {
        super(bigQuery, dataSetName, tableName);
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public void setUid_mudLogs(String uid_mudLogs) {
        this.uid_mudLogs = uid_mudLogs;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setdTime(DateTime dTime) {
        this.dTime = dTime;
    }

    public void setMdTop(Float mdTop) {
        this.mdTop = mdTop;
    }

    public void setMdTop_uom(String mdTop_uom) {
        this.mdTop_uom = mdTop_uom;
    }

    public void setMdBottom(Float mdBottom) {
        this.mdBottom = mdBottom;
    }

    public void setMdBottom_uom(String mdBottom_uom) {
        this.mdBottom_uom = mdBottom_uom;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setCommonTime_dTimCreation(DateTime commonTime_dTimCreation) {
        this.commonTime_dTimCreation = commonTime_dTimCreation;
    }

    public void setCommonTime_dTimLastChange(DateTime commonTime_dTimLastChange) {
        this.commonTime_dTimLastChange = commonTime_dTimLastChange;
    }

    public void save() {
        TableRow data = new TableRow();
        data.set("uid", this.uid);
        data.set("uid_mudLogs", this.uid_mudLogs);
        data.set("type", this.type);
        data.set("dTime", this.dTime);
        data.set("mdTop", this.mdTop);
        data.set("mdTop_uom", this.mdTop_uom);
        data.set("mdBottom", this.mdBottom);
        data.set("mdBottom_uom", this.mdBottom_uom);
        data.set("text", this.text);
        data.set("commonTime_dTimCreation", this.commonTime_dTimCreation);
        data.set("commonTime_dTimLastChange", this.commonTime_dTimLastChange);

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
