package com.bia.sandbox;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.Util.WitsmlMarshal;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLogs;
import org.apache.xalan.processor.TransformerFactoryImpl;

import javax.xml.bind.JAXBException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        String mudlogsXML = null;
        try {
            mudlogsXML = getReturnData("1411/mudLog.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }

        ObjMudLogs mudlogs = null;
        try {
            mudlogs = WitsmlMarshal.deserialize(mudlogsXML, ObjMudLogs.class);
            System.out.println(mudlogs.getMudLog().size());
            System.out.println(mudlogs.getMudLog().get(0).getUid());
        } catch (JAXBException ex) {
            ex.printStackTrace();
        }

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("bia-energy")
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("/home/asatrya/Documents/gdrive-gmail/01-BIA/Credentials/bia-energy-abf693d2bbec.json")))
                .build().getService();

        // Create tables
        TableId tableId = TableId.of("witsml", "mudLog");

        // Table field definition
        List<Field> fields = new ArrayList<>(
                Arrays.asList(
                        Field.of("nameWell", LegacySQLTypeName.STRING),
                        Field.of("nameWellbore", LegacySQLTypeName.STRING)
                )
        );

        // Table schema definition
        Schema schema = Schema.of(fields);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
    }

    private static String prettyFormat(String input, int indent) {
        try {
            Source xmlInput = new StreamSource(new StringReader(input));
            StringWriter stringWriter = new StringWriter();
            StreamResult xmlOutput = new StreamResult(stringWriter);
            TransformerFactory transformerFactory = new TransformerFactoryImpl();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private static String getReturnData(String resourcePath) throws IOException {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream));
        return reader.lines().collect(Collectors.joining(
                System.getProperty("line.separator")));
    }
}
