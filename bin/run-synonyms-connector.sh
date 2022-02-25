#!/usr/bin/env bash
java \
    -cp ./lib/sadasystems-gcs-synonyms-connector-1.0-jar-with-dependencies.jar \
    com.sadasystems.gcs.connector.synonyms.SynonymsConnector \
    -Dconfig=./properties/synonyms-connector.properties \
    -Djava.util.logging.config.file=./properties/logging.properties

