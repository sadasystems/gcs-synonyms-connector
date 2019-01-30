#!/usr/bin/env bash
java \
    -cp ./lib/sada-cloudsearch-synonyms-connector-v1.0-SNAPSHOT-withlib.jar \
    com.sadasystems.gcs.connector.servicenow.ServiceNowConnector \
    -Dconfig=./properties/servicenow-connector.properties \
    -Djava.util.logging.config.file=./properties/logging.properties --verbose

