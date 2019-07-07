package com.bia.sandbox;

import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlMarshal;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsMudLogParameter;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLogs;
import org.apache.commons.cli.*;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, JAXBException, ParseException {

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

            // Read WITSML value
            String mudlogsXML = Files.lines(Paths.get(xmlFile)).collect(Collectors.joining("\n"));
            ObjMudLogs mudLogsObj = WitsmlMarshal.deserialize(mudlogsXML, ObjMudLogs.class);

            // Create BigQuery connection
            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(gcpProjectName)
                    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(gcpKeyPath)))
                    .build().getService();

            // check if table exists, create new one if not
            if(! isExisting(bigquery, dataSetName, tableName)){

                System.out.println("Table " + tableName + " is not exist.");

                // // Table field definition
                // List<Field> fields = new ArrayList<>(Arrays.asList(Field.of("nameWell", LegacySQLTypeName.STRING),
                // Field.of("nameWellbore", LegacySQLTypeName.STRING)));

                // // Table schema definition
                // Schema schema = Schema.of(fields);
                // TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                // TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                // Table table = bigquery.create(tableInfo);
                // System.out.println("Table " + table.getFriendlyName() + " has been created.");
            }else{
                System.out.println("Number of mudLog=" + mudLogsObj.getMudLog().size());

                Iterator<ObjMudLog> mudLogsIter = mudLogsObj.getMudLog().iterator();
                while(mudLogsIter.hasNext()){
                    ObjMudLog mudLogObj = mudLogsIter.next();

                    // main mudLogBQ object
                    MudLogBQ mudLogBQ = new MudLogBQ(bigquery, dataSetName, "mudLogs");
                    mudLogBQ.setUid(mudLogObj.getUid());
                    mudLogBQ.setUidWell(mudLogObj.getUidWell());
                    mudLogBQ.setUidWellbore(mudLogObj.getUidWellbore());
                    mudLogBQ.setNameWell(mudLogObj.getNameWell());
                    mudLogBQ.setNameWellbore(mudLogObj.getNameWellbore());
                    mudLogBQ.setName(mudLogObj.getName());
                    mudLogBQ.setdTim(new DateTime(mudLogObj.getDTim().toGregorianCalendar().getTime()));
                    mudLogBQ.setMudLogCompany(mudLogObj.getMudLogCompany());
                    mudLogBQ.setMudLogEngineers(mudLogObj.getMudLogEngineers());
                    mudLogBQ.setStartMd(new Float(mudLogObj.getStartMd().getValue()));
                    mudLogBQ.setStartMd_uom(mudLogObj.getStartMd().getUom().value());
                    mudLogBQ.setEndMd(new Float(mudLogObj.getEndMd().getValue()));
                    mudLogBQ.setEndMd_uom(mudLogObj.getEndMd().getUom().value());
                    mudLogBQ.setCommonData_dTimCreation(
                            new DateTime(mudLogObj.getCommonData().getDTimCreation().toGregorianCalendar().getTime()));
                    mudLogBQ.setCommonData_dTimLastChange(
                            new DateTime(mudLogObj.getCommonData().getDTimLastChange().toGregorianCalendar().getTime()));
                    mudLogBQ.setCommonData_itemState(mudLogObj.getCommonData().getItemState().value());
                    mudLogBQ.setCommonData_defaultDatum(mudLogObj.getCommonData().getDefaultDatum().getValue());

                    // save mudLogBQ
                    System.out.println("Insert mudLogObj uid=" + mudLogObj.getUid());
                    mudLogBQ.save();

                    // parameters
                    Iterator<CsMudLogParameter> csMudLogParameterIterator = mudLogObj.getParameter().iterator();
                    System.out.println("Number of parameters=" + mudLogObj.getParameter().size());
                    while (csMudLogParameterIterator.hasNext()) {
                        CsMudLogParameter csMudLogParameter = csMudLogParameterIterator.next();

                        MudLogParameterBQ mudLogParameterBQ = new MudLogParameterBQ(bigquery, dataSetName, "mudLogs_parameters");
                        mudLogParameterBQ.setUid(csMudLogParameter.getUid());
                        mudLogParameterBQ.setUid_mudLogs(mudLogBQ.getUid());
                        mudLogParameterBQ.setType(csMudLogParameter.getType().value());
//                        try {
//                            mudLogParameterBQ.setdTime(new DateTime(
//                                    csMudLogParameter.getDTime().toGregorianCalendar().getTime()));
//                        }catch (NullPointerException e){
//                            e.printStackTrace();
//                        }
                        mudLogParameterBQ.setMdTop(new Float(csMudLogParameter.getMdTop().getValue()));
                        mudLogParameterBQ.setMdTop_uom(csMudLogParameter.getMdTop().getUom().value());
                        mudLogParameterBQ.setMdBottom(new Float(csMudLogParameter.getMdBottom().getValue()));
                        mudLogParameterBQ.setMdBottom_uom(csMudLogParameter.getMdBottom().getUom().value());
                        mudLogParameterBQ.setText(csMudLogParameter.getText());
                        mudLogParameterBQ.setCommonTime_dTimCreation(new DateTime(
                                csMudLogParameter.getCommonTime().getDTimCreation().toGregorianCalendar().getTime()));
                        mudLogParameterBQ.setCommonTime_dTimLastChange(new DateTime(
                                csMudLogParameter.getCommonTime().getDTimCreation().toGregorianCalendar().getTime()));

                        // save parameter
                        System.out.println("Insert parameter uid=" + mudLogParameterBQ.getUid());
                        mudLogParameterBQ.save();
                    }
                }

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
