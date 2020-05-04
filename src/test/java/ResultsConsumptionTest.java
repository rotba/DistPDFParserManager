import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResultsConsumptionTest extends MainTest {

    private Task.NewTask newT;
    private ResultsConsumption out;
    private String finalOutputBucket;
    private String finalOutputKey;
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
        finalOutputBucket = "rotemb271-test-final-output" + new Date().getTime();
        finalOutputKey = "rotemb271FinalOutputKey" + new Date().getTime();
        s3.createBucket(CreateBucketRequest.builder().bucket(resultsBucket).build());
        s3.createBucket(CreateBucketRequest.builder().bucket(finalOutputBucket).build());
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(resultsSqsName)
                        .build()
        );
        resultsSqsUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(resultsSqsName).build()).queueUrl();
        out = new ResultsConsumption(
                resultsSqsName,
                resultsBucket,
                finalOutputBucket,
                finalOutputKey,
                Region.US_EAST_1,
                Main.generateInfoLogger(),
                Main.generateSeverLogger()
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        theOutThread.interrupt();
        tearDownSqs(resultsSqsName);
        tearDownBucket(resultsBucket, finalOutputKey);
        tearDownBucket(finalOutputBucket, finalOutputKey);
    }

    private File download(String outputBucket, String outputKey) {
        Runtime rt = Runtime.getRuntime();
        try {
            String address = String.format("https://%s.s3.amazonaws.com/%s", outputBucket, outputKey);
            Path pathPrefix = Paths.get(System.getProperty("user.dir"), "test_files", "output");
            Process pr = rt.exec(String.format("wget %s -P %s", address, pathPrefix));
            pr.waitFor();
            return new File(Paths.get(pathPrefix.toString(), outputKey).toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        fail();
        return null;
    }

    private boolean htmlContains(File download, String s) {
        try {
            Document doc = Jsoup.parse(
                    download,
                    "utf-8"
            );
            String text = doc.body().text();
            return text.contains(s);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

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
                                        "-u", "https://rotemb271-test-output-bucket2.s3.amazonaws.com/jesusandseder.png"
                                )
                        )
                        .build()
        );
        theOutThread = new Thread(out);
        theOutThread.start();
        out.sealConsumption();
        Utils.waitDispatchWorker();
        assertTrue(
                htmlContains(
                        download(finalOutputBucket, finalOutputKey),
                        "ToImage http://www.jewishfederations.org/local_includes/downloads/39497.pdf https://rotemb271-test-output-bucket2.s3.amazonaws.com/jesusandseder.png")
        );
    }
}