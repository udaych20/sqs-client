package com.wipro.aws.sqs.test.sqsclient;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SqsTest {

    private static SqsClient sqsClient = null;

    private static final String role_arn = "arn:aws:iam::<<acc-id>>:role/test-role";

    private static final Region region = Region.US_EAST_2;

    public SqsTest() throws IOException {
        final List<String> queues = getQueues();

        queues.parallelStream().forEach(url -> {

        });
        this.sqsClient = createSqsClient(authenticateAwsConfiguration(role_arn));
        String json = "{\"array\":[{\"name\":\"uday\",\"test\":[{\"company\":\"wipro\"}]},{\"number\":1}]}";
        String log = "2023-05-30T13:25:11,889 [main] INFO  org.opensearch.dataprepper.pipeline.server.DataPrepperServer - Data Prepper server running at :4900";
        String testMsg = "Test Message";

        List<String> messageList = new ArrayList<>();
        messageList.add(json);
        // Can add more types for testing
//        messageList.add(log);
//        messageList.add(testMsg);
        while(true)
        messageList.forEach(message -> {
            queues.parallelStream().forEach(url -> {
                SendMessageRequest request = SendMessageRequest.builder()
                        .messageBody(message)
                        .queueUrl(url).build();
                final SendMessageResponse sendMessageResponse = sqsClient.sendMessage(request);
                System.out.println("response -> " + sendMessageResponse.messageId() + " ,Message: " + message);
            });
        });
    }

    public List<String> getQueues(){
        List<String> list = new ArrayList<>();
        // Add the list of sqs queue urls here
        list.add("https://sqs.us-east-2.amazonaws.com/<<ACC ID>>/my-queue");
        return list;
    }

    SqsClient createSqsClient(final AwsCredentialsProvider awsCredentialsProvider) {
        return SqsClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

    public AwsCredentialsProvider authenticateAwsConfiguration(final String awsStsRoleArn) {
        final AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {
            final StsClient stsClient = StsClient.builder()
                    .region(region)
                    .build();
            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("aqs-sqs-common-" + UUID.randomUUID())
                    .roleArn(awsStsRoleArn);

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequestBuilder.build())
                    .build();
        } else {
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }
        return awsCredentialsProvider;
    }
    public static void main(String[] args) throws IOException {
        new SqsTest();
    }
}
