import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class OperationProductionTest extends MainTest{

    private Task.NewTask newT;
    private OperationsProduction out;
    private String action;
    private String inputFile;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        for (Bucket b:
        s3.listBuckets(ListBucketsRequest.builder().build()).buckets()) {
            for (S3Object s3o:
                 s3.listObjects(ListObjectsRequest.builder().bucket(b.name()).build()).contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(b.name()).key(s3o.key()).build());
            }
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(b.name()).build());
        }
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
        action = "ToImage";
        inputFile = "http://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf";
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(tasksSqsName).build()).queueUrl())
                        .messageBody(
                                String.format(
                                        "%s\t%s",
                                        action, inputFile
                                )
                        )
                        .build()
        );
//        Manager outManager = new Manager(tasksSqsName,Main.WORKER_AMI,Main.WORKER_TAG,Main.generateInfoLogger(),Main.generateSeverLogger());
        s3.createBucket(CreateBucketRequest.builder().bucket(tasksBucket).build());
        s3.createBucket(CreateBucketRequest.builder().bucket(operationsBucket).build());
        taskInputKey = "rotemb271TestInputKey" + new Date().getTime();
        newT = new Task.NewTask(
                tasksBucket,
                taskInputKey,
                finalOutputKey,
                Message.builder().body("TEST").build()
        );
        out = new OperationsProduction(
                operationSqsName,
                resultsSqsName,
                operationsBucket,
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
        tearDownBucket(operationsBucket);
        tearDownBucket(tasksBucket);
    }

    @Test
    public void testOneFileTheWorkerGetsValidOperation() throws IOException {
        out.handleNewTask(newT);
        Utils.waitDispatchWorker();
        String lastGivenTS = out.getLastGivenTimeStamp();
        assertTrue(
                sqsContainsOperation(
                        operationSqsName,
                        new String[]{" ",
                                "-a", action,
                                "-i", inputFile,
                                "-b", operationsBucket,
                                "-k", lastGivenTS,
                                "-t", "TRYING_TO_AVOID",
                                "-fb", tasksBucket,
                                "-fk", finalOutputKey
                        })
        );
    }
}