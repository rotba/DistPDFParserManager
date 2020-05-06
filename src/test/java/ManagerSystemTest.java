import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ManagerSystemTest extends MainTest {


    private Thread theOutThread;
    private String tasksSQSQUrl;
    private String finalOutputKey;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(tasksSqsName)
                        .build()
        );
        tasksSQSQUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder()
                        .queueName(tasksSqsName)
                        .build()
        ).queueUrl();
        s3.createBucket(
                CreateBucketRequest.builder()
                        .bucket(tasksBucket)
                        .build()
        );
        finalOutputKey = "rotemb271TestFinalOutputKey"+new Date().getTime();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        tearDownSqs(tasksSqsName);
        theOutThread.interrupt();
    }

//    private int countWorkers() {
//        return out.checkNumberOfWorkers();
//    }

    @Test
    public void testManager() throws IOException, InterruptedException {
        String newTask = String.join(" ",
                "-t", "new_task",
                "-b", tasksBucket,
                "-ki", taskInputKey,
                "-ko", finalOutputKey
        );
        String terminate = String.join(" ",
                "-t", "terminate",
                "-b", tasksBucket,
                "-ki", taskInputKey,
                "-ko", finalOutputKey
        );
        theOutThread = new Thread(() ->
        {
            try {
                Main.main(new String[]{
                        "-i", tasksSqsName,
                        "-kid", IGNOREMECREDENTIALS.kid,
                        "-sak", IGNOREMECREDENTIALS.sak
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        theOutThread.start();
        s3.putObject(
                PutObjectRequest.builder().acl("public-read").bucket(tasksBucket).key(taskInputKey).build(),
                Paths.get(System.getProperty("user.dir"), "test_files", "test_input_nw_one_operation.txt")
        );
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(tasksSQSQUrl)
                        .messageBody(newTask)
                        .build()
        );
        Thread.sleep(100*1000);
//        sqs.sendMessage(
//                SendMessageRequest.builder()
//                        .queueUrl(tasksSQSQUrl)
//                        .messageBody(terminate)
//                        .build()
//        );
        Thread.sleep(20*1000);
        assertTrue(
                Utils.htmlContains(
                        Utils.download(tasksBucket, finalOutputKey),
                        String.format(
                                "ToImage http://www.bethelnewton.org/images/Passover_Guide_BOOKLET.pdf https://%s.s3.amazonaws.com/%s",
                                Manager.FORTETSINGgetOperationsBucket(),""
                        ))
        );
    }
}