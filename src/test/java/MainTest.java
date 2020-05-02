import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertTrue;

public class MainTest {
    private SqsClient sqs;
    private Ec2Client ec2;
    private String tasksSqsName;
    private Instant testStartTime;
    private Thread theMainThread;

    @Before
    public void setUp() throws Exception {
        testStartTime = Instant.now();
        tasksSqsName = "rotemb271WorkerMainTest" + new Date().getTime();
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(tasksSqsName)
                        .build()
        );
        ec2 = Ec2Client.builder().build();
    }

    @After
    public void tearDown() throws Exception {
        theMainThread.interrupt();
        tearDownSqs(tasksSqsName);
        tearDownWorker();
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
                        "-t" ,tasksSqsName
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        theMainThread.start();
        Utils.waitDispatchWorker();
        assertTrue(aNewWorkerExist());
    }

    @Test
    public void testOneFileTheWorkerGetsValidOperation() throws IOException {
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(tasksSqsName).build()).queueUrl())
                        .messageBody("ToImage\thttp://www.jewishfederations.org/local_includes/downloads/39497.pdf")
                        .build()
        );
        Manager outManager = new Manager(tasksSqsName,Main.WORKER_AMI,Main.WORKER_TAG,Main.generateInfoLogger(),Main.generateSeverLogger());
        Operation operation = outManager.getNextOperation();
        outManager.produceOperation(operation);
        Utils.waitDispatchWorker();
        assertTrue(
                sqsContainsOperation(
                        outManager.getOperationsSqsName(),
                        new String[]{" ",
                                "-a", operation.getAction(),
                                "-i", operation.getInput(),
                                "-b", operation.getS3Bucket(),
                                "-k",operation.getS3Key(),
                                "-t", operation.getInstant()
                        })
        );
    }
}