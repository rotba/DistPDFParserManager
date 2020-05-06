import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResultsConsumptionTest extends MainTest {

    private Task.NewTask newT;
    private ResultsConsumption out;
    private String resultsSqsUrl;
    private Thread theOutThread;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(resultsSqsName)
                        .build()
        );
        s3.createBucket(CreateBucketRequest.builder().bucket(operationsBucket).build());
        s3.createBucket(CreateBucketRequest.builder().bucket(tasksBucket).build());
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(resultsSqsName)
                        .build()
        );
        resultsSqsUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(resultsSqsName).build()).queueUrl();
        out = new ResultsConsumption(
                resultsSqsName,
                operationsBucket,
                new AtomicInteger(0),
                Region.US_EAST_1,
                new Manager.PendingTasksSocket() {
                    @Override
                    public Integer numberOfPendingOperationsLeft(String b, String k) {
                        return 1;
                    }

                    @Override
                    public void operationFulfilled(String b, String k) {

                    }
                },
                Main.generateInfoLogger(),
                Main.generateSeverLogger()
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        theOutThread.interrupt();
        tearDownSqs(resultsSqsName);
        tearDownBucket(operationsBucket);
        tearDownBucket(tasksBucket);
    }

    @Test
    public void testOneFileTheWorkerGetsValidOperation() throws IOException {
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(resultsSqsUrl)
                        .messageBody(
                                String.join(" ",
                                        "-a", "ToImage",
                                        "-i", "http://www.jewishfederations.org/local_includes/downloads/39497.pdf",
                                        "-s", "SUCCESS",
                                        "-t", "TRYINGTOAVOID",
                                        "-d", "NOT_TESTING",
                                        "-u", "https://rotemb271-test-output-bucket2.s3.amazonaws.com/jesusandseder.png",
                                        "-b", tasksBucket,
                                        "-k", finalOutputKey
                                )
                        )
                        .build()
        );
        theOutThread = new Thread(out);
        theOutThread.start();
        Utils.waitDispatchWorker();
        out.sealConsumption(tasksBucket, finalOutputKey);
        assertTrue(
                Utils.htmlContains(
                        Utils.download(tasksBucket, finalOutputKey),
                        "ToImage http://www.jewishfederations.org/local_includes/downloads/39497.pdf https://rotemb271-test-output-bucket2.s3.amazonaws.com/jesusandseder.png")
        );
    }
}