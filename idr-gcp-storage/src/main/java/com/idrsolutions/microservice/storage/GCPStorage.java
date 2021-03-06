package com.idrsolutions.microservice.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.*;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link Storage} that uses GCP Cloud Storage to store files
 */
public class GCPStorage extends BaseStorage {
    private final Storage storage;

    private final String projectID;
    private final String bucketName;
    private final String basePath;

    /**
     * Finds the credentials file using the Environment Variable: GOOGLE_APPLICATION_CREDENTIALS
     * @param projectID The project ID Containing the bucket
     * @param bucketName The name of the bucket that the converted files should be uploaded to
     */
    public GCPStorage(final String projectID, final String bucketName, final String basePath) {
        // Will fetch from the "GOOGLE_APPLICATION_CREDENTIALS" environment variable
        this.storage = StorageOptions.newBuilder().setProjectId(projectID).build().getService();
        this.projectID = projectID;
        this.bucketName = bucketName;
        this.basePath = basePath;
    }

    /**
     * Initialises with a user provided credentials file
     * @param credentialsPath The path to a file containing the credentials
     * @param projectID The project ID Containing the bucket
     * @param bucketName The name of the bucket that the converted files should be uploaded to
     * @throws IOException if it cannot find or access the credentialsPath
     */
    public GCPStorage(final String credentialsPath, final String projectID, final String bucketName, final String basePath) throws IOException {
        final GoogleCredentials credentials;

        try (FileInputStream fileStream = new FileInputStream(credentialsPath)) {
            credentials = GoogleCredentials.fromStream(fileStream).createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        }

        this.storage = StorageOptions.newBuilder().setProjectId(projectID).setCredentials(credentials).build().getService();
        this.projectID = projectID;
        this.bucketName = bucketName;
        this.basePath = basePath;
    }

    public GCPStorage(final Properties properties) throws IOException {
        // storageprovider.gcp.credentialspath
        // storageprovider.gcp.projectid
        // storageprovider.gcp.bucketname

        final String error = validateProperties(properties);
        if (!error.isEmpty()) {
            throw new IllegalStateException(error);
        }

        final GoogleCredentials credentials;

        try (FileInputStream fileStream = new FileInputStream(properties.getProperty("storageprovider.gcp.credentialspath"))) {
            credentials = GoogleCredentials.fromStream(fileStream).createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        }

        this.projectID = properties.getProperty("storageprovider.gcp.projectid");
        this.storage = StorageOptions.newBuilder().setProjectId(projectID).setCredentials(credentials).build().getService();
        this.bucketName = properties.getProperty("storageprovider.gcp.bucketname");
        this.basePath = properties.getProperty("storageprovider.gcp.basepath", "");

        this.storage.getServiceAccount(projectID);
    }

    /**
     * @inheritDoc
     */
    @Override
    public String put(final byte[] fileToUpload, final String fileName, final String uuid) {
        final String basePath = !this.basePath.isEmpty() ? this.basePath + "/" : "";
        final Blob blob = storage.create(BlobInfo.newBuilder(bucketName, basePath + uuid + "/" + fileName).build(), fileToUpload, Storage.BlobTargetOption.detectContentType());
        return blob.signUrl(30, TimeUnit.MINUTES, Storage.SignUrlOption.withV4Signature()).toString();
    }

    @Override
    public String put(final InputStream fileToUpload, final long fileSize, final String fileName, final String uuid) {
        try {
            final String basePath = !this.basePath.isEmpty() ? this.basePath + "/" : "";
            final Blob blob = storage.createFrom(BlobInfo.newBuilder(bucketName, basePath + uuid + "/" + fileName).build(), fileToUpload, Storage.BlobWriteOption.detectContentType());
            return blob.signUrl(30, TimeUnit.MINUTES, Storage.SignUrlOption.withV4Signature()).toString();
        } catch (final IOException e) {
            LOG.severe("Failed to upload to GCP: " + e.getMessage());
        }
        return null;
    }

    private String validateProperties(final Properties propertiesFile) {
        String error = "";

        // storageprovider.gcp.credentialspath
        String credentialspath = propertiesFile.getProperty("storageprovider.gcp.credentialspath");
        if (credentialspath == null || credentialspath.isEmpty()) {
            error += "storageprovider.gcp.credentialspath must have a value\n";
        } else {
            if (credentialspath.startsWith("~")) {
                credentialspath = System.getProperty("user.home") + credentialspath.substring(1);
                propertiesFile.setProperty("storageprovider.gcp.credentialspath", credentialspath);
            }
            File credentialsFile = new File(credentialspath);
            if (!credentialsFile.exists() || !credentialsFile.isFile() || !credentialsFile.canRead()) {
                error += "storageprovider.gcp.credentialspath must point to a valid credentials file that can be accessed";
            }
        }

        // storageprovider.gcp.projectid
        final String projectid = propertiesFile.getProperty("storageprovider.gcp.projectid");
        if (projectid == null || projectid.isEmpty()) {
            error += "storageprovider.gcp.projectid must have a value\n";
        }

        // storageprovider.gcp.bucketname
        final String bucketname = propertiesFile.getProperty("storageprovider.gcp.bucketname");
        if (bucketname == null || bucketname.isEmpty()) {
            error += "storageprovider.gcp.bucketname must have a value\n";
        }

        return error;
    }
}
