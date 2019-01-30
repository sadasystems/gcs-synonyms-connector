package com.sadasystems.gcs.connector.synonyms;

import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

public class SynonymsConnector {

    public static void main(String[] args) throws IOException, InterruptedException {

        // create the checkpointDirectory if it doesn't exist
        if (!Configuration.isInitialized()) {
            Configuration.initConfig(args);
            String checkpointDirectory = Configuration.getString("connector.checkpointDirectory", "").get();
            if (StringUtils.isNotBlank(checkpointDirectory)) {
                FileUtils.forceMkdir(new File(checkpointDirectory));
            }
        }

        IndexingApplication application = new IndexingApplication.Builder(
                new FullTraversalConnector(new SynonymsRepository()), args).build();
        application.start();
    }
}
