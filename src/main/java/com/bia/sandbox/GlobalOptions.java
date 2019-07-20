package com.bia.sandbox;

public class GlobalOptions {
    private static GlobalOptions instance = null;

    private String witsmlObject;
    private String xmlFile;
    private String gcpProjectName;
    private String gcpKeyPath;
    private String dataSetName;

    private GlobalOptions(String witsmlObject, String xmlFile, String gcpProjectName, String gcpKeyPath, String dataSetName){
        this.witsmlObject = witsmlObject;
        this.xmlFile = xmlFile;
        this.gcpProjectName = gcpProjectName;
        this.gcpKeyPath = gcpKeyPath;
        this.dataSetName = dataSetName;
    };

    public static void initialize(String witsmlObject, String xmlFile, String gcpProjectName, String gcpKeyPath, String dataSetName){
        instance = new GlobalOptions(witsmlObject, xmlFile, gcpProjectName, gcpKeyPath, dataSetName);
    }

    public static GlobalOptions getInstance() throws NullPointerException {
        if(instance == null){
            throw new NullPointerException();
        }else {
            return instance;
        }
    }

    public String getWitsmlObject() {
        return witsmlObject;
    }

    public String getXmlFile() {
        return xmlFile;
    }

    public String getGcpProjectName() {
        return gcpProjectName;
    }

    public String getGcpKeyPath() {
        return gcpKeyPath;
    }

    public String getDataSetName() {
        return dataSetName;
    }
}