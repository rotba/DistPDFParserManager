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
import java.util.logging.Level;

import static org.junit.Assert.*;

public class MainTest {
    private SqsClient sqs;
    private String queueUrl;
    private Instant testStarTime;
    private Main out;
    private Thread theMainThread;

    @Before
    public void setUp() throws Exception {
        theMainThread =new Thread(()-> {
            try {
                Main.main(new String[]{});
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        testStarTime = Instant.now();
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(Main.TASKE_QUEUE_NAME)
                .build();
        CreateQueueResponse createResult = sqs.createQueue(request);
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(Main.TASKE_QUEUE_NAME)
                .build();
        queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
    }

    @After
    public void tearDown() throws Exception {
        theMainThread.interrupt();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        for (Message m : sqs.receiveMessage(receiveRequest).messages()) {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest);
        }
    }

    private boolean aNewWorkerExist() {
        final Ec2Client ec2 = Ec2Client.builder().build();
        boolean done = false;
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        while(!done) {
            DescribeInstancesResponse response = ec2.describeInstances(request);
            for(Reservation reservation : response.reservations()) {
                for(Instance instance : reservation.instances()) {
                    for (Tag t:instance.tags()) {
                        if (t.key().equals("Name") &&t.value().equals(Main.WORKER_TAG)){
                            if (instance.launchTime().isAfter(testStarTime)){
                                return true;
                            }
                        }
                    }
                }
            }
            if(response.nextToken() == null) {
                done = true;
            }
        }
        return false;
    }

    @Test
    public void testOneFileInSQSCheckOneWorkerHasBeenCreated() {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("ToImage\thttp://www.jewishfederations.org/local_includes/downloads/39497.pdf")
                .build();
        theMainThread.start();
        sqs.sendMessage(sendMessageRequest);
        Utils.waitDispatchWorker();
        assertTrue(aNewWorkerExist());
    }
}