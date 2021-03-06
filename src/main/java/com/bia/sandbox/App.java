package com.bia.sandbox;

import com.bia.sandbox.config.GlobalOptions;
import com.bia.sandbox.mudlog.*;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlMarshal;
import com.hashmapinc.tempus.WitsmlObjects.v1411.*;
import org.apache.commons.cli.*;

import javax.xml.bind.JAXBException;
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
        CommandLine cmd = parser.parse(options, args);
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

        GlobalOptions.initialize(witsmlObject, xmlFile, gcpProjectName, gcpKeyPath, dataSetName);

        if (witsmlObject.equals("mudLogs")) {

            String tableName = "mudLogs";

            // Read WITSML value
            String mudlogsXML = Files.lines(Paths.get(xmlFile)).collect(Collectors.joining("\n"));
            ObjMudLogs mudLogsObj = WitsmlMarshal.deserialize(mudlogsXML, ObjMudLogs.class);

            // check if table exists, create new one if not
            //if(! isExisting(bigquery, dataSetName, tableName)){
            if (false) {

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
            } else {
                System.out.println("Number of mudLog=" + mudLogsObj.getMudLog().size());

                Iterator<ObjMudLog> mudLogsIter = mudLogsObj.getMudLog().iterator();
                while (mudLogsIter.hasNext()) {
                    ObjMudLog mudLogObj = mudLogsIter.next();

                    // main mudLogDaoImpl object
                    MudLogDao mudLogDaoImpl = new MudLogDaoImpl();
                    System.out.println("Insert mudLogObj uid=" + mudLogObj.getUid());
                    mudLogDaoImpl.save(mudLogObj);

                    // parameters
                    Iterator<CsMudLogParameter> csMudLogParameterIterator = mudLogObj.getParameter().iterator();
                    System.out.println("Number of parameters=" + mudLogObj.getParameter().size());
                    while (csMudLogParameterIterator.hasNext()) {
                        CsMudLogParameter csMudLogParameter = csMudLogParameterIterator.next();

                        ParameterDao mudLogParameterImpl = new ParameterDaoImpl();
                        System.out.println("Insert parameter uid=" + csMudLogParameter.getUid());
                        mudLogParameterImpl.save(csMudLogParameter, mudLogObj);
                    }

                    // geologyInterval
                    Iterator<CsGeologyInterval> csGeologyIntervalIterator = mudLogObj.getGeologyInterval().iterator();
                    System.out.println("Number of geologyInterval=" + mudLogObj.getGeologyInterval().size());
                    while (csGeologyIntervalIterator.hasNext()) {
                        CsGeologyInterval csGeologyInterval = csGeologyIntervalIterator.next();

                        GeologyIntervalDao geologyIntervalDaoImpl = new GeologyIntervalDaoImpl();
                        System.out.println("Insert GeologyInterval; uid=" + csGeologyInterval.getUid());
                        geologyIntervalDaoImpl.save(csGeologyInterval, mudLogObj);

                        // lithology
                        Iterator<CsLithology> csLithologyIterator = csGeologyInterval.getLithology().iterator();
                        System.out.println("Number of lithology=" + csGeologyInterval.getLithology().size());
                        while(csLithologyIterator.hasNext()){
                            CsLithology csLithology = csLithologyIterator.next();

                            LithologyDao lithologyDaoImpl = new LithologyDaoImpl();
                            System.out.println("Insert Lithology; uid=" + csLithology.getUid());
                            lithologyDaoImpl.save(csLithology, csGeologyInterval);

                            // qualifier
                            Iterator<CsQualifier> csQualifierIterator = csLithology.getQualifier().iterator();
                            System.out.println("Number of qualifier=" + csLithology.getQualifier().size());
                            while(csQualifierIterator.hasNext()){
                                CsQualifier csQualifier = csQualifierIterator.next();

                                QualifierDao qualifierDaoImpl = new QualifierDaoImpl();
                                System.out.println("Insert Qualifiery; uid=" + csQualifier.getUid());
                                qualifierDaoImpl.save(csQualifier, csLithology);
                            }
                        }

                        // Lithostratigraphic
                        Iterator<LithostratigraphyStruct> lithostratigraphyStructIter = csGeologyInterval.getLithostratigraphic().iterator();
                        System.out.println("Number of lithostratigraphy=" + csGeologyInterval.getLithostratigraphic().size());
                        while (lithostratigraphyStructIter.hasNext()){
                            LithostratigraphyStruct lithostratigraphyStruct = lithostratigraphyStructIter.next();

                            LithostatigraphicDao lithostatigraphicDaoImpl = new LithostatigraphicDaoImpl();
                            System.out.println("Insert Lithostatigraphic; kind/value=" + lithostratigraphyStruct.getValue());
                            lithostatigraphicDaoImpl.save(lithostratigraphyStruct, csGeologyInterval);
                        }

                        // Chronostatigraphic
                        Iterator<ChronostratigraphyStruct> chronostratigraphyStructIterator =
                                csGeologyInterval.getChronostratigraphic().iterator();
                        System.out.println("Number of Chronostratigraphic=" + csGeologyInterval.getChronostratigraphic().size());
                        while (chronostratigraphyStructIterator.hasNext()){
                            ChronostratigraphyStruct chronostratigraphyStruct = chronostratigraphyStructIterator.next();

                            ChronostatigraphicDao chronostatigraphicDaoImpl = new ChronostatigraphicDaoImpl();
                            System.out.println("Insert Chronostatigraphic; kind/value="+ chronostratigraphyStruct.getValue());
                            chronostatigraphicDaoImpl.save(chronostratigraphyStruct, csGeologyInterval);
                        }

                        // NameFormation
                        Iterator<String> nameFormationIterator =
                                csGeologyInterval.getNameFormation().iterator();
                        System.out.println("Number of NameFormation=" + csGeologyInterval.getNameFormation().size());
                        while (nameFormationIterator.hasNext()){
                            String nameFormation = nameFormationIterator.next();

                            NameFormationDao nameFormationDaoImpl =
                                    new NameFormationDaoImpl();
                            System.out.println("Insert NameFormation; value=" + nameFormation);
                            nameFormationDaoImpl.save(nameFormation, csGeologyInterval);
                        }
                    }
                }

            }

        } else {
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
