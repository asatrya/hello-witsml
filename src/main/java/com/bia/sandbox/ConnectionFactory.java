package com.bia.sandbox;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import java.io.FileInputStream;
import java.io.IOException;

public class ConnectionFactory {
    /**
     * Get a connection to database
     * @return Connection object
     */
    public static BigQuery getConnection() throws NullPointerException, IOException {
        String gcpProjectName = GlobalOptions.getInstance().getGcpProjectName();
        String gcpKeyPath = GlobalOptions.getInstance().getGcpKeyPath();

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(gcpProjectName)
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(gcpKeyPath)))
                .build().getService();
        return bigquery;
    }
}
