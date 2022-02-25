package com.sadasystems.gcs.connector.synonyms;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkState;


public class SynonymsRepository implements Repository {

    private static final Logger logger = Logger.getLogger(SynonymsRepository.class.getName());

    private IndexingService.RequestMode requestMode = IndexingService.RequestMode.ASYNCHRONOUS;

    private static final String SYNONYMS_FILE_PATH = "synonymsFile.path";
    private static final String SYNONYMS_FILE_URL = "synonymsFile.url";
    public static final String ONLY_APPLICABLE_FOR_ATTACHED_SEARCH_APPLICATIONS = "onlyApplicableForAttachedSearchApplications";

    private String synonymsFilePath;
    private String synonymsFileUrl;
    private boolean onlyApplicableForAttachedSearchApplications;

    public SynonymsRepository() {
    }

    @Override
    public void init(RepositoryContext context) throws RepositoryException {
        checkState(com.google.enterprise.cloudsearch.sdk.config.Configuration.isInitialized(), "configuration not initialized");

        synonymsFilePath = Configuration.getString(SYNONYMS_FILE_PATH, "").get();
        synonymsFileUrl = Configuration.getString(SYNONYMS_FILE_URL, "").get();
        onlyApplicableForAttachedSearchApplications = Configuration.getBoolean(ONLY_APPLICABLE_FOR_ATTACHED_SEARCH_APPLICATIONS, false).get();
    }


    /**
     * Not used. getChanges will do a full traversal the first time it is called
     *
     * @return iterator of ServiceNow records converted to docs
     * @throws RepositoryException on access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getAllDocs(byte[] checkpoint) throws RepositoryException {

        CSVFormat csvFormat = CSVFormat.RFC4180.withIgnoreEmptyLines()
                .withIgnoreSurroundingSpaces()
                .withFirstRecordAsHeader()
                .withCommentMarker('#');

        Reader reader = null;

        if (StringUtils.isNotBlank(synonymsFilePath)) {
            if (Files.exists(Paths.get(synonymsFilePath))) {
                try {
                    reader = new BufferedReader(new FileReader(synonymsFilePath));
                } catch (FileNotFoundException e) {
                    throw new RepositoryException.Builder().setErrorMessage("Error processing Synonyms File: " + synonymsFilePath).setCause(e).build();
                }
            } else {
                throw new RepositoryException.Builder()
                        .setErrorMessage("Synonyms file not found: " + synonymsFilePath)
                        .build();
            }
        }
        if (StringUtils.isNotBlank(synonymsFileUrl)) {
            try {
                reader = new BufferedReader(new InputStreamReader(new URL(synonymsFileUrl).openStream()));
            } catch (IOException e) {
                throw new RepositoryException.Builder().setErrorMessage("Error processing Synonyms File URL: " + synonymsFileUrl).setCause(e).build();
            }
        }

        if(reader == null) {
            throw new RepositoryException.Builder()
                    .setErrorType(RepositoryException.ErrorType.CLIENT_ERROR)
                    .setErrorMessage("No Synonym File Path or URL defined on configuration")
                    .build();
        }

        try {
            CSVParser parser = new CSVParser(reader, csvFormat);
            List<ApiOperation> allDocs = StreamSupport.stream(parser.spliterator(), false)
                    .map(this::buildDocument)
                    .collect(Collectors.toList());
            return new CheckpointCloseableIterableImpl.Builder<>(allDocs).build();

        } catch (IOException e) {
            throw new RepositoryException.Builder()
                    .setCause(e)
                    .setErrorType(RepositoryException.ErrorType.CLIENT_ERROR)
                    .build();
        }
    }

    /**
     * Creates a document for indexing.
     * <p>
     * For this connector sample, the created document is domain public
     * searchable. The content is a simple text string.
     *
     * @param record The current CSV record to convert
     * @return the fully formed document ready for indexing
     */
    private ApiOperation buildDocument(CSVRecord record) {
        // Extract term and synonyms from record
        String term = record.get(0);
        List<String> synonyms = StreamSupport.stream(record.spliterator(), false)
                .skip(1) // Skip term
                .filter(item-> !item.isEmpty())
                .collect(Collectors.toList());

        Multimap<String, Object> structuredData = ArrayListMultimap.create();
        structuredData.put("_term", term);
        structuredData.putAll("_synonym", synonyms);
        structuredData.put("_onlyApplicableForAttachedSearchApplications", onlyApplicableForAttachedSearchApplications);

        String itemName = String.format("dictionary/%s", term);

        Acl acl = new Acl.Builder().setReaders(Collections.singletonList(Acl.getCustomerPrincipal())).build();


        // Using the SDK item builder class to create the item
        Item item = IndexingItemBuilder.fromConfiguration(itemName)
                .setItemType(IndexingItemBuilder.ItemType.CONTENT_ITEM)
                .setObjectType("_dictionaryEntry")
                .setValues(structuredData)
                .setAcl(acl)
                .build();

        // Create the fully formed document
        return new RepositoryDoc.Builder()
                .setItem(item)
                .setRequestMode(requestMode)
                .build();
    }

    @Override
    public CheckpointCloseableIterable<ApiOperation> getChanges(byte[] checkpoint) throws RepositoryException {
        return null;
    }

    @Override
    public CheckpointCloseableIterable<ApiOperation> getIds(@Nullable byte[] checkpoint) throws
            RepositoryException {
        return null;
    }

    @Override
    public ApiOperation getDoc(Item item) {
        return null;
    }

    @Override
    public boolean exists(Item item) {
        return false;
    }

    @Override
    public void close() {
    }


}
