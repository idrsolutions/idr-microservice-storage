package com.idrsolutions.microservice.storage;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.util.Properties;

/**
 * An implementation of {@link IStorage} that uses DigitalOcean Spaces to store files
 * This reuses {@link AWSStorage} by chaining the endpoint as the API is compatible
 */
public class DigitalOceanStorage extends AWSStorage {
    /**
     * Uses Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
     * @param region The DigitalOcean Region
     * @param bucketName The name of the bucket in AWS that the converted files should be uploaded to
     * @param basePath The path inside the bucket that the converted files should end up in
     */
    DigitalOceanStorage(final String region, final String bucketName, final String basePath) {
        super(AmazonS3ClientBuilder.standard().withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://" + region + ".digitaloceanspaces.com", region)).build(), bucketName, basePath);
    }

    /**
     * Allows passing any form of AWS authentication
     * @param region The DigitalOcean Region
     * @param credentialsProvider The user credentials for DigitalOcean
     * @param bucketName The name of the bucket in AWS that the converted files should be uploaded to
     * @param basePath The path inside the bucket that the converted files should end up in
     */
    public DigitalOceanStorage(final String region, final AWSCredentialsProvider credentialsProvider, final String bucketName, final String basePath) {
        super(AmazonS3ClientBuilder.standard().withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://" + region + ".digitaloceanspaces.com", region)).withCredentials(credentialsProvider).build(), bucketName, basePath);
    }

    public DigitalOceanStorage(Properties properties) {
        // storageprovider.do.region
        // storageprovider.do.accesskey
        // storageprovider.do.secretkey
        // storageprovider.do.bucketname

        String error = validateProperties(properties);
        if (!error.isEmpty()) {
            throw new IllegalStateException(error);
        }

        s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://" + properties.getProperty("storageprovider.do.region") + ".digitaloceanspaces.com", properties.getProperty("storageprovider.do.region")))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(properties.getProperty("storageprovider.do.accesskey"), properties.getProperty("storageprovider.do.secretkey"))))
                .build();
        bucketName = properties.getProperty("storageprovider.do.bucketname");
        basePath = properties.getProperty("storageprovider.do.basepath", "");

        if (!s3Client.doesBucketExistV2(properties.getProperty("storageprovider.aws.bucketname"))) {
            throw new RuntimeException("A bucket with the name " + properties.getProperty("storageprovider.aws.bucketname") + " does not exist");
        }
    }

    private String validateProperties(Properties propertiesFile) {
        String error = "";

        // storageprovider.do.region
        String region = propertiesFile.getProperty("storageprovider.do.region");
        if (region == null || region.isEmpty()) {
            error += "storageprovider.do.region must have a value\n";
        }

        // storageprovider.do.accesskey
        String accesskey = propertiesFile.getProperty("storageprovider.do.accesskey");
        if (accesskey == null || accesskey.isEmpty()) {
            error += "storageprovider.do.accesskey must have a value\n";
        }

        // storageprovider.do.secretkey
        String secretkey = propertiesFile.getProperty("storageprovider.do.secretkey");
        if (secretkey == null || secretkey.isEmpty()) {
            error += "storageprovider.do.secretkey must have a value\n";
        }

        // storageprovider.do.bucketname
        String bucketname = propertiesFile.getProperty("storageprovider.do.bucketname");
        if (bucketname == null || bucketname.isEmpty()) {
            error += "storageprovider.do.bucketname must have a value\n";
        }

        return error;
    }
}
