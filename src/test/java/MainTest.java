import org.apache.commons.cli.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertTrue;

public class MainTest {
    private SqsClient sqs;
    private Ec2Client ec2;
    private S3Client s3;
    private String tasksSqsName;
    private String s3TasksBucket;
    private Instant testStartTime;
    private Thread theMainThread;
    private String operationSqsName;
    private String resultsSqsName;
    private String outputBucket;

    @Before
    public void setUp() throws Exception {
        testStartTime = Instant.now();
        tasksSqsName = "rotemb271WorkerMainTest" + new Date().getTime();
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        ec2 = Ec2Client.builder().build();
        s3TasksBucket = "rotemb271-test-task-bucket" + new Date().getTime();
        s3 = S3Client.create();
        operationSqsName = "rotemb271TestOperationsSqs" + new Date().getTime();
        resultsSqsName = "rotemb271TestresultsSqs" + new Date().getTime();
        outputBucket = "rotemb271-test-results"+new Date().getTime();
    }

    @After
    public void tearDown() throws Exception {
        if(theMainThread!=null)
            theMainThread.interrupt();
        tearDownSqs(tasksSqsName);
        tearDownSqs(operationSqsName);
        tearDownBucket(outputBucket);
        tearDownBucket(s3TasksBucket);
        tearDownWorker();
    }

    private void tearDownBucket(String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucket)
                .build();
        s3.deleteBucket(deleteBucketRequest);
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

    private void tearDownSqs(String sqsName) {
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

    @Test
    public void testOneFileInSQSCheckOneWorkerHasBeenCreated() {
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(tasksSqsName).build()).queueUrl())
                        .messageBody("ToImage\thttp://www.jewishfederations.org/local_includes/downloads/39497.pdf")
                        .build()
        );
        theMainThread = new Thread(() -> {
            try {
                Main.main(new String[]{
                        "-t", tasksSqsName
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        theMainThread.start();
        Utils.waitDispatchWorker();
        assertTrue(aNewWorkerExist());
    }

    private boolean equivalentCommands(String body, String[] expected) {
        Options operationParsingOptions = new Options();
        Option action = new Option("a", "action", true, "action");
        action.setRequired(true);
        operationParsingOptions.addOption(action);
        Option input = new Option("i", "input", true, "input file");
        input.setRequired(true);
        operationParsingOptions.addOption(input);
        Option status = new Option("s", "status", true, "status");
        status.setRequired(true);
        operationParsingOptions.addOption(status);
        Option url = new Option("u", "url", true, "result url");
        url.setRequired(true);
        operationParsingOptions.addOption(url);
        Option timestamp = new Option("t", "timestamp", true, "timestamp");
        timestamp.setRequired(true);
        operationParsingOptions.addOption(timestamp);
        Option description = new Option("d", "description", true, "description");
        description.setRequired(true);
        operationParsingOptions.addOption(description);
        CommandLineParser operationParser = new DefaultParser();
        try {
            CommandLine expectedCmd = operationParser.parse(operationParsingOptions, expected);
            CommandLine operationResultCmd = operationParser.parse(operationParsingOptions, body.split("\\s+"));
            return expectedCmd.getOptionValue("a").equals(operationResultCmd.getOptionValue("a")) &&
                    expectedCmd.getOptionValue("i").equals(operationResultCmd.getOptionValue("i")) &&
                    expectedCmd.getOptionValue("s").equals(operationResultCmd.getOptionValue("s")) &&
                    expectedCmd.getOptionValue("u").equals(operationResultCmd.getOptionValue("u"));
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean sqsContainsOperation(String pushNotoficationsSqs, String[] command) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(pushNotoficationsSqs)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        for (Message m:sqs.receiveMessage(receiveRequest).messages()) {
            if(equivalentCommands(m.body(), command))
                return true;
        }
        return false;
    }

    @Test
    public void testOneFileTheWorkerGetsValidOperation() throws IOException {
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(tasksSqsName)
                        .build()
        );
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(operationSqsName)
                        .build()
        );
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(tasksSqsName).build()).queueUrl())
                        .messageBody("ToImage\thttp://www.jewishfederations.org/local_includes/downloads/39497.pdf")
                        .build()
        );
//        Manager outManager = new Manager(tasksSqsName,Main.WORKER_AMI,Main.WORKER_TAG,Main.generateInfoLogger(),Main.generateSeverLogger());
        s3.createBucket(CreateBucketRequest.builder().bucket(s3TasksBucket).build());
        s3.createBucket(CreateBucketRequest.builder().bucket(outputBucket).build());
        OperationsProduction op = new OperationsProduction(
                operationSqsName,
                resultsSqsName,
                outputBucket,
                0,
                null,
                Main.generateInfoLogger(),
                Main.generateSeverLogger()
        );
        String inputKey = "rotemb271TestInputKey" + new Date().getTime();
        Task.NewTask newT = new Task.NewTask(
                s3TasksBucket,
                inputKey,
                null,
                Message.builder().body("TEST").build()
        );
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(newT.getBucket())
                .key(newT.getKeyInput())
                .acl("public-read")
                .build();
        op.handleNewTask(newT);
        s3.putObject(
                putObjectRequest,
                Paths.get(System.getProperty("user.dir"),"test_files", "test_input_nw_one_operation.txt")
        );
        Utils.waitDispatchWorker();
        assertTrue(
                sqsContainsOperation(
                        operationSqsName,
                        new String[]{" ",
                                "-a", "ToImage",
                                "-i", "http://www.jewishfederations.org/local_includes/downloads/39497.pdf",
                                "-b", outputBucket,
                                "-k", "http://www.jewishfederations.org/local_includes/downloads/39497.pdf",
                                "-t", "TRYING_TO_AVOID"
                        })
        );
    }
}