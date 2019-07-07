package com.bia.sandbox;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;

public abstract class BaseModelBQ {
    protected String TABLE_NAME;
    protected String uid;
    protected BigQuery bigQuery;
    protected TableId tableId;

    public BaseModelBQ(BigQuery bigQuery, String dataSetName, String tableName) {
        this.bigQuery = bigQuery;
        this.TABLE_NAME = tableName;
        this.tableId = TableId.of(dataSetName, this.TABLE_NAME);
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
