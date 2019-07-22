# Hello WITSML
This is a simple application to demonstrate reading WITSML file, process it, and save it to BigQuery.

## Build JAR
```
mvn clean install
```

## Run the main class
```
mvn exec:java -Dexec.mainClass="com.bia.sandbox.App" -Dexec.args="\
    -o <witsm-object> \
    -f <xml-path> \
    -p <gcp-project-id> \
    -k <gcp-key-json-path> \
    -d <bigquery-dataset>"
```

Options explanation:

* **witsml-object**: WITSML object type. For now, the only supported is `mudLogs`
* **xml-path**: path to input XML file. For `mudLogs` object, the example file is `file/mudLogs.xml`
* **gcp-project-id**: your Google Cloud Platform project ID
* **gcp-key-json-path**: your Google Cloud Platform service account's JSON key
* **bigquery-dataset**: your BigQuery dataset name
