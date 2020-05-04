import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class OperationProductionTest extends MainTest{

    private Task.NewTask newT;
    private OperationsProduction out;

    @Before
    public void setUp() throws Exception {
        super.setUp();
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
        s3.createBucket(CreateBucketRequest.builder().bucket(operationsResultsAndTasksResultsBucket).build());
        taskInputKey = "rotemb271TestInputKey" + new Date().getTime();
        newT = new Task.NewTask(
                s3TasksBucket,
                taskInputKey,
                null,
                Message.builder().body("TEST").build()
        );
        out = new OperationsProduction(
                operationSqsName,
                resultsSqsName,
                operationsResultsAndTasksResultsBucket,
                new AtomicInteger(),
                null,
                Region.US_EAST_1,
                Main.generateInfoLogger(),
                Main.generateSeverLogger()
        );
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(newT.getBucket())
                .key(newT.getKeyInput())
                .acl("public-read")
                .build();
        s3.putObject(
                putObjectRequest,
                Paths.get(System.getProperty("user.dir"), "test_files", "test_input_nw_one_operation.txt")
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        tearDownSqs(tasksSqsName);
        tearDownSqs(operationSqsName);
        tearDownBucket(operationsResultsAndTasksResultsBucket, operationResultKey);
        tearDownBucket(s3TasksBucket, taskInputKey);
    }

    @Test
    public void testOneFileTheWorkerGetsValidOperation() throws IOException {
        out.handleNewTask(newT);
        Utils.waitDispatchWorker();
        operationResultKey = "http://www.jewishfederations.org/local_includes/downloads/39497.pdf";
        assertTrue(
                sqsContainsOperation(
                        operationSqsName,
                        new String[]{" ",
                                "-a", "ToImage",
                                "-i", operationResultKey,
                                "-b", operationsResultsAndTasksResultsBucket,
                                "-k", operationResultKey,
                                "-t", "TRYING_TO_AVOID"
                        })
        );
    }
}