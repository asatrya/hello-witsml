package com.bia.sandbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlMarshal;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLogs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, InterruptedException, ParseException {

        Options options = new Options();
        options.addOption("o", true, "WITSML object name (ex: mudLogs)");
        options.addOption("f", true, "WITSML file as input");
        options.addOption("p", true, "GCP project name");
        options.addOption("k", true, "GCP service account key");
        options.addOption("d", true, "BigQuery dataset name");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);
        String witsmlObject = cmd.getOptionValue("o");
        String xmlFile = cmd.getOptionValue("f");
        String gcpProjectName = cmd.getOptionValue("p");
        String gcpKeyPath = cmd.getOptionValue("k");
        String dataSetName = cmd.getOptionValue("d");

        System.out.println("Read WITSML object: " + witsmlObject);
        System.out.println("Read XML file: " + xmlFile);
        System.out.println("Read GCP project name: " + gcpProjectName);
        System.out.println("Read GCP service account key: " + gcpKeyPath);
        System.out.println("Read BigQuery dataset: " + dataSetName);

        if(witsmlObject.equals("mudLogs")){

            String tableName = "mudLogs";

            String mudlogsXML = null;
            mudlogsXML = Files.lines(Paths.get(xmlFile)).collect(Collectors.joining("\n"));

            ObjMudLogs mudlogs = null;
            try {
                mudlogs = WitsmlMarshal.deserialize(mudlogsXML, ObjMudLogs.class);
                System.out.println(mudlogs.getMudLog().size());
                System.out.println(mudlogs.getMudLog().get(0).getUid());
            } catch (JAXBException ex) {
                ex.printStackTrace();
            }

            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(gcpProjectName)
                    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(gcpKeyPath)))
                    .build().getService();

            // Create tables
            TableId tableId = TableId.of(dataSetName, tableName);

            // check if table exists, create new one if not
            if(! isExisting(bigquery, dataSetName, tableName)){
                // Table field definition
                List<Field> fields = new ArrayList<>(Arrays.asList(Field.of("nameWell", LegacySQLTypeName.STRING),
                Field.of("nameWellbore", LegacySQLTypeName.STRING)));

                // Table schema definition
                Schema schema = Schema.of(fields);
                TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                Table table = bigquery.create(tableInfo);
                System.out.println("Table " + table.getFriendlyName() + " has been created.");
            }else{
                System.out.println("Table " + tableName + " is already exist.");
            }

        }else{
            System.out.println("Object " + witsmlObject + " is not supported yet.");
        }
    }

    public static boolean isExisting(BigQuery bqObject, String dataSetName, String tableName) {
        return getDataset(bqObject, dataSetName).get(tableName) != null;
    }
    
    protected static Dataset getDataset(BigQuery bqObject, String dataSetName) {
        return bqObject.getDataset(dataSetName);
    }
}
