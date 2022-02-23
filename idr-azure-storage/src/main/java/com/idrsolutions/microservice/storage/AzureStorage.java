package com.idrsolutions.microservice.storage;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.OffsetDateTime;
import java.util.Properties;

/**
 * An implementation of {@link Storage} that uses Azure Blob Storage to store files
 */
public class AzureStorage extends BaseStorage {
    final BlobServiceClient client;

    private final String accountName;
    private final String containerName;
    private final String basePath;

    /**
     * Authenticates using Shared Key
     * @param auth The authentication for Azure
     * @param accountName The storage account name
     * @param containerName The name of the container within the storage account that the converted files should be uploaded to
     */
    public AzureStorage(final StorageSharedKeyCredential auth, final String accountName, final String containerName, final String basePath) {
        this.client = new BlobServiceClientBuilder().credential(auth).endpoint("https://" + accountName + ".blob.core.windows.net").buildClient();
        this.accountName = accountName;
        this.containerName = containerName;
        this.basePath = basePath;
    }

    /**
     * Authenticates using SAS
     * @param auth The authentication for Azure
     * @param accountName The storage account name
     * @param containerName The name of the container within the storage account
     */
    public AzureStorage(final AzureSasCredential auth, final String accountName, final String containerName, final String basePath) {
        this.client = new BlobServiceClientBuilder().credential(auth).endpoint("https://" + accountName + ".blob.core.windows.net").buildClient();
        this.accountName = accountName;
        this.containerName = containerName;
        this.basePath = basePath;
    }

    /**
     * Authenticates using a Token
     * @param auth The authentication for Azure
     * @param accountName The storage account name
     * @param containerName The name of the container within the storage account
     */
    public AzureStorage(final TokenCredential auth, final String accountName, final String containerName, final String basePath) {
        this.client = new BlobServiceClientBuilder().credential(auth).endpoint("https://" + accountName + ".blob.core.windows.net").buildClient();
        this.accountName = accountName;
        this.containerName = containerName;
        this.basePath = basePath;
    }

    public AzureStorage(final Properties properties) {
        // storageprovider.azure.accountname
        // storageprovider.azure.accountkey
        // storageprovider.azure.containername
        // storageprovider.azure.basepath

        final String error = validateProperties(properties);
        if (!error.isEmpty()) {
            throw new IllegalStateException(error);
        }
        this.accountName = properties.getProperty("storageprovider.azure.accountname");
        final StorageSharedKeyCredential auth = new StorageSharedKeyCredential(accountName,
                properties.getProperty("storageprovider.azure.accountkey"));
        this.client = new BlobServiceClientBuilder().credential(auth).endpoint("https://" + accountName + ".blob.core.windows.net").buildClient();

        this.containerName = properties.getProperty("storageprovider.azure.containername");
        this.basePath = properties.getProperty("storageprovider.azure.basepath", "");

        this.client.getAccountInfo();
    }

    /**
     * @inheritDoc
     */
    @Override
    public String put(final byte[] fileToUpload, final String fileName, final String uuid) {
        try (InputStream fileStream = new ByteArrayInputStream(fileToUpload)){
            return put(fileStream, fileToUpload.length, fileName, uuid);
        } catch (final IOException e) {
            LOG.severe(e.getMessage());
            return null;
        }
    }

    @Override
    public String put(final InputStream fileToUpload, final long fileSize,  final String fileName, final String uuid) {
        BlobContainerClient containerClient;
        try {
            containerClient = client.createBlobContainer(containerName);
        } catch (final BlobStorageException e) {
            // The container may already exist, so don't throw an error
            if (!e.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
                LOG.severe(e.getMessage());
                return null;
            }
            containerClient = client.getBlobContainerClient(containerName);
        }


        final String basePath = !this.basePath.isEmpty() ? this.basePath + "/" : "";
        final String dest = basePath + uuid + "/" + fileName;

        final BlockBlobClient blobClient = containerClient.getBlobClient(dest).getBlockBlobClient();

        blobClient.upload(fileToUpload, fileSize);

        // Set the filename using the content disposition HTTP Header to avoid the downloaded file also containing the UUID
        blobClient.setHttpHeaders(new BlobHttpHeaders().setContentDisposition("attachment; filename=" + fileName));

        final BlobServiceSasSignatureValues sas = new BlobServiceSasSignatureValues(OffsetDateTime.now().plusMinutes(30), new BlobSasPermission().setReadPermission(true));

        final String test = blobClient.generateSas(sas);

        final URL blobURL = new BlobUrlParts()
                .setScheme("https://")
                .setHost(accountName + ".blob.core.windows.net")
                .setContainerName(containerName)
                .setBlobName(dest)
                .parseSasQueryParameters(test)
                .toUrl();

        return blobURL.toString();
    }

    private String validateProperties(final Properties propertiesFile) {
        String error = "";

        // storageprovider.azure.accountname
        final String accountname = propertiesFile.getProperty("storageprovider.azure.accountname");
        if (accountname == null || accountname.isEmpty()) {
            error += "storageprovider.azure.accountname must have a value\n";
        }

        // storageprovider.azure.accountkey
        final String accountkey = propertiesFile.getProperty("storageprovider.azure.accountkey");
        if (accountkey == null || accountkey.isEmpty()) {
            error += "storageprovider.azure.accountkey must have a value\n";
        }

        // storageprovider.azure.containername
        final  String containername = propertiesFile.getProperty("storageprovider.azure.containername");
        if (containername == null || containername.isEmpty()) {
            error += "storageprovider.azure.containername must have a value\n";
        }

        return error;
    }
}
