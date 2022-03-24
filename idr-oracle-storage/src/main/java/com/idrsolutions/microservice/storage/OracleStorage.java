package com.idrsolutions.microservice.storage;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreatePreauthenticatedRequestDetails;
import com.oracle.bmc.objectstorage.model.PreauthenticatedRequest;
import com.oracle.bmc.objectstorage.requests.CreatePreauthenticatedRequestRequest;
import com.oracle.bmc.objectstorage.requests.GetBucketRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.CreatePreauthenticatedRequestResponse;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * An implementation of {@link Storage} that uses Oracle Buckets to store files
 */
public class OracleStorage extends BaseStorage {
    private final ObjectStorageClient client;

    private final String namespace;
    private final String bucketName;
    private final String basePath;

    /**
     * Authenticates using the
     * @param region The Oracle Region
     * @param configFilePath The path to the OCI config file containing the authentication profiles, use null for the default file at "~/.oci/config"
     * @param profile The profile inside the config file, use null for the default profile
     * @param namespace The namespace of the bucket
     * @param bucketName the name of the bucket that the converted files should be uploaded to
     * @throws IOException if the credentials file is inaccessible
     */
    public OracleStorage(final Region region, final String configFilePath, final @Nullable String profile, final String namespace, final String bucketName, final String basePath) throws IOException {
        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parse(configFilePath, profile);

        final AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile);

        this.client = new ObjectStorageClient(provider);
        this.client.setRegion(region);

        this.namespace = namespace;
        this.bucketName = bucketName;
        this.basePath = basePath;
    }

    /**
     * Authenticates using the provided implementation of {@link com.oracle.bmc.auth.BasicAuthenticationDetailsProvider}
     * @param region The Oracle Region
     * @param auth The user credentials for Oracle Cloud
     * @param namespace The namespace of the bucket
     * @param bucketName the name of the bucket that the converted files should be uploaded to
     */
    public OracleStorage(final Region region, final BasicAuthenticationDetailsProvider auth, final String namespace, final String bucketName, final String basePath) {
        this.client = new ObjectStorageClient(auth);
        this.client.setRegion(region);

        this.namespace = namespace;
        this.bucketName = bucketName;
        this.basePath = basePath;
    }

    public OracleStorage(final Properties properties) throws IOException {
        // storageprovider.oracle.region
        // storageprovider.oracle.ociconfigfilepath
        // storageprovider.oracle.profile (nullable)
        // storageprovider.oracle.namespace
        // storageprovider.oracle.bucketname
        // storageprovider.oracle.basepath

        final String error = validateProperties(properties);
        if (!error.isEmpty()) {
            throw new IllegalStateException(error);
        }

        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parse(properties.getProperty("storageprovider.oracle.ociconfigfilepath"),
                !properties.getProperty("storageprovider.oracle.profile", "").trim().isEmpty() ? properties.getProperty("storageprovider.oracle.profile") : null);

        final AuthenticationDetailsProvider provider = new ConfigFileAuthenticationDetailsProvider(configFile);

        this.client = new ObjectStorageClient(provider);
        this.client.setRegion(Region.fromRegionId(properties.getProperty("storageprovider.oracle.region")));

        this.namespace = properties.getProperty("storageprovider.oracle.namespace");
        this.bucketName = properties.getProperty("storageprovider.oracle.bucketname");
        this.basePath = properties.getProperty("storageprovider.oracle.basepath", "");

        this.client.getBucket(GetBucketRequest.builder().bucketName(bucketName).namespaceName(namespace).build());
    }

    /**
     * @inheritDoc
     */
    @Override
    public String put(final byte[] fileToUpload, final String fileName, final String uuid) {
        try (InputStream fileStream = new ByteArrayInputStream(fileToUpload)) {
            return put(fileStream, fileToUpload.length, fileName, uuid);
        } catch (final IOException e) {
            LOG.severe(e.getMessage());
        }
        return null;
    }

    @Override
    public String put(final InputStream fileToUpload, final long fileSize, final String fileName, final String uuid) {
        final String basePath = !this.basePath.isEmpty() ? this.basePath + "/" : "";
        final String dest = basePath + uuid + "/" + fileName;

        final PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucketName(bucketName)
                .namespaceName(namespace)
                .objectName(dest)
                .contentLength(fileSize)
                .putObjectBody(fileToUpload)
                .build();

        client.putObject(objectRequest);

        final CreatePreauthenticatedRequestDetails preauthenticatedDetails = CreatePreauthenticatedRequestDetails.builder()
                .name("Converted PDF " + dest + " Download")
                .objectName(dest)
                .accessType(CreatePreauthenticatedRequestDetails.AccessType.ObjectRead)
                .timeExpires(new Date(System.currentTimeMillis() + 60 * 30 * 1000))
                .bucketListingAction(PreauthenticatedRequest.BucketListingAction.Deny)
                .build();

        final CreatePreauthenticatedRequestRequest preauthenticatedRequest = CreatePreauthenticatedRequestRequest.builder()
                .bucketName(bucketName)
                .namespaceName(namespace)
                .createPreauthenticatedRequestDetails(preauthenticatedDetails)
                .build();

        final CreatePreauthenticatedRequestResponse response = client.createPreauthenticatedRequest(preauthenticatedRequest);

        return client.getEndpoint() + response.getPreauthenticatedRequest().getAccessUri();
    }

    private String validateProperties(final Properties propertiesFile) {
        String error = "";

        // storageprovider.oracle.ociconfigfilepath
        String ociconfigfilepath = propertiesFile.getProperty("storageprovider.oracle.ociconfigfilepath");
        if (ociconfigfilepath == null || ociconfigfilepath.isEmpty()) {
            error += "storageprovider.oracle.ociconfigfilepath must have a value\n";
        } else {
            if (ociconfigfilepath.startsWith("~")) {
                ociconfigfilepath = System.getProperty("user.home") + ociconfigfilepath.substring(1);
                propertiesFile.setProperty("storageprovider.oracle.ociconfigfilepath", ociconfigfilepath);
            }
            File configFile = new File(ociconfigfilepath);
            if (!configFile.exists() || !configFile.isFile() || !configFile.canRead()) {
                error += "storageprovider.oracle.ociconfigfilepath must point to a valid config file that can be accessed";
            }
        }

        // storageprovider.oracle.region
        final String region = propertiesFile.getProperty("storageprovider.oracle.region");
        if (region == null || region.isEmpty()) {
            error += "storageprovider.oracle.region must have a value\n";
        } else {
            try {
                // Region.fromRegionId(region);
            } catch (final IllegalArgumentException e) {
                error += "storageprovider.oracle.region has been set to an unknown region, please check you have entered the region correctly\n";
            }
        }

        // storageprovider.oracle.namespace
        final String namespace = propertiesFile.getProperty("storageprovider.oracle.namespace");
        if (namespace == null || namespace.isEmpty()) {
            error += "storageprovider.oracle.namespace must have a value\n";
        }

        // storageprovider.oracle.bucketname
        final String bucketname = propertiesFile.getProperty("storageprovider.oracle.bucketname");
        if (bucketname == null || bucketname.isEmpty()) {
            error += "storageprovider.oracle.bucketname must have a value\n";
        }

        return error;
    }
}
