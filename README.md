Build JAR
```
mvn clean install
```

Run the main class
```
mvn exec:java -Dexec.mainClass="com.bia.sandbox.App" -Dexec.args="\
    -o mudLogs \
    -f data/mudLogs.xml \
    -p bia-energy \
    -k /home/asatrya/Documents/gdrive-gmail/01-BIA/Credentials/bia-energy-abf693d2bbec.json \
    -d witsml"
```
