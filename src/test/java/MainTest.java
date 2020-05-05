import org.apache.commons.cli.*;
import org.junit.After;
import org.junit.Before;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertTrue;

public abstract class MainTest {
    protected SqsClient sqs;
    protected Ec2Client ec2;
    protected S3Client s3;
    protected String tasksSqsName;
    protected String tasksBucket;
    protected Instant testStartTime;
    protected Thread theMainThread;
    protected String operationSqsName;
    protected String resultsSqsName;
    protected String taskInputKey;
    protected String operationResultKey;
    protected String operationsBucket;
    protected String finalOutputKey;

    @Before
    public void setUp() throws Exception {
        testStartTime = Instant.now();
        tasksSqsName = "rotemb271WorkerMainTest" + new Date().getTime();
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        ec2 = Ec2Client.builder().build();
        tasksBucket = "rotemb271-test-tasks-bucket" + new Date().getTime();
        s3 = S3Client.builder().build();
        operationSqsName = "rotemb271TestOperationsSqs" + new Date().getTime();
        resultsSqsName = "rotemb271TestresultsSqs" + new Date().getTime();
        operationsBucket = "rotemb271-test-operations-bucket" + new Date().getTime();
        taskInputKey = "rotemb271TestTaskInputKey"+new Date().getTime();
        finalOutputKey = "rotemb271FinalOutputKey" + new Date().getTime();
    }

    @After
    public void tearDown() throws Exception {
        if (theMainThread != null)
            theMainThread.interrupt();
        tearDownWorker();
    }

    protected void tearDownBucket(String bucket) {
        for (S3Object s3o:
             s3.listObjects(ListObjectsRequest.builder().bucket(bucket).build()).contents()) {
            s3.deleteObject(
                    DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3o.key())
                            .build()
            );
        }
        s3.deleteBucket(
                DeleteBucketRequest.builder()
                        .bucket(bucket)
                        .build()
        );
    }

    private void tearDownWorker() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        boolean done = false;
        while (!done) {
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    for (Tag t : instance.tags()) {
                        if (t.key().equals("Name") && t.value().equals(Main.WORKER_TAG)) {
                            if (instance.launchTime().isAfter(testStartTime)) {
                                TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                                        .instanceIds(instance.instanceId())
                                        .build();

                                ec2.terminateInstances(terminateRequest);
                            }
                        }
                    }
                }
            }
            if (response.nextToken() == null) {
                done = true;
            }
        }
    }

    protected void tearDownSqs(String sqsName) {
        String queueUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder()
                        .queueName(sqsName)
                        .build()
        ).queueUrl();

        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(deleteQueueRequest);
    }

    private boolean aNewWorkerExist() {
        boolean done = false;
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        while (!done) {
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    for (Tag t : instance.tags()) {
                        if (t.key().equals("Name") && t.value().equals(Main.WORKER_TAG)) {
                            if (instance.launchTime().isAfter(testStartTime)) {
                                return true;
                            }
                        }
                    }
                }
            }
            if (response.nextToken() == null) {
                done = true;
            }
        }
        return false;
    }

//    @Test
//    public void testOneFileInSQSCheckOneWorkerHasBeenCreated() {
//        sqs.sendMessage(
//                SendMessageRequest.builder()
//                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(tasksSqsName).build()).queueUrl())
//                        .messageBody("ToImage\thttp://www.jewishfederations.org/local_includes/downloads/39497.pdf")
//                        .build()
//        );
//        theMainThread = new Thread(() -> {
//            try {
//                Main.main(new String[]{
//                        "-t", tasksSqsName
//                });
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        theMainThread.start();
//        Utils.waitDispatchWorker();
//        assertTrue(aNewWorkerExist());
//    }

    private boolean equivalentCommands(String body, String[] expected) {
        Options operationParsingOptions = new Options();
        Option action = new Option("a", "action", true, "action");
        action.setRequired(true);
        operationParsingOptions.addOption(action);
        Option input = new Option("i", "input", true, "input file");
        input.setRequired(true);
        operationParsingOptions.addOption(input);
        Option bucket = new Option("b", "bucket", true, "bucket");
        bucket.setRequired(true);
        operationParsingOptions.addOption(bucket);
        Option key = new Option("k", "key", true, "key");
        key.setRequired(true);
        operationParsingOptions.addOption(key);
        Option timestamp = new Option("t", "timestamp", true, "timestamp");
        timestamp.setRequired(true);
        operationParsingOptions.addOption(timestamp);
        Option finalBucket = new Option("fb", "finalbucket", true, "finalbucket");
        finalBucket.setRequired(true);
        operationParsingOptions.addOption(finalBucket);
        Option finalKey = new Option("fk", "finalkey", true, "finalkey");
        finalKey.setRequired(true);
        operationParsingOptions.addOption(finalKey);
        CommandLineParser operationParser = new DefaultParser();
        try {
            CommandLine expectedCmd = operationParser.parse(operationParsingOptions, expected);
            CommandLine operationResultCmd = operationParser.parse(operationParsingOptions, body.split("\\s+"));
            return expectedCmd.getOptionValue("a").equals(operationResultCmd.getOptionValue("a")) &&
                    expectedCmd.getOptionValue("i").equals(operationResultCmd.getOptionValue("i")) &&
                    expectedCmd.getOptionValue("b").equals(operationResultCmd.getOptionValue("b")) &&
                    expectedCmd.getOptionValue("k").equals(operationResultCmd.getOptionValue("k"))&&
                    expectedCmd.getOptionValue("fb").equals(operationResultCmd.getOptionValue("fb")) &&
                    expectedCmd.getOptionValue("fk").equals(operationResultCmd.getOptionValue("fk"));
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    protected boolean sqsContainsOperation(String pushNotoficationsSqs, String[] command) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        for (Message m : sqs.receiveMessage(receiveRequest).messages()) {
            if (equivalentCommands(m.body(), command))
                return true;
        }
        return false;
    }
}