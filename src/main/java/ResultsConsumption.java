import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.cli.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ResultsConsumption implements Runnable {
    private final String resultsSqsName;
    private final String resultsBucket;
    private final String finalOutputBucket;
    private final String finalOutputKey;
    private final Region usEast1;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private final String resultsSQSUrl;
    private S3Client s3;
    private SqsClient sqs;
    private List<Result> results;
    private Boolean working = true;

    public ResultsConsumption(String resultsSqsName, String resultsBucket, String finalOutputBucket, String finalOutputKey, Region usEast1, InfoLogger infoLogger, SeverLogger severLogger) {
        this.resultsSqsName = resultsSqsName;
        this.resultsBucket = resultsBucket;
        this.finalOutputBucket = finalOutputBucket;
        this.finalOutputKey = finalOutputKey;
        this.usEast1 = usEast1;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        s3 = S3Client.create();
        sqs = SqsClient.create();
        this.resultsSQSUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder().queueName(resultsSqsName).build()
        ).queueUrl();
        results = new ArrayList<>();
    }

    @Override
    public void run() {
        infoLogger.log("Start consuming results");
        boolean workingVal;
        synchronized (this.working) {
            workingVal = working.booleanValue();
        }
        while (workingVal) {
            try {
                ReceiveMessageResponse receiveMessageResponse;
                do {
                    receiveMessageResponse = sqs.receiveMessage(
                            ReceiveMessageRequest.builder()
                                    .queueUrl(resultsSQSUrl)
                                    .build()
                    );
                } while (receiveMessageResponse.messages().size() == 0);
                for (Message m : receiveMessageResponse.messages()) {
                    consume(m);
                    delete(m);
                }
            } catch (ParseException e) {
                severLogger.log("Fail parsing the message. Leeps comsuming", e);
            }
            synchronized (this.working) {
                workingVal = working.booleanValue();
            }
        }
    }

    private void store(Result r) {
        results.add(r);
    }

    public void sealConsumption() {
        synchronized (this.working) {
            working = false;
        }
        Document doc = Jsoup.parse("<html></html>");
        doc.body().addClass("body-styles-cls");
        for (Result r : results)
            doc.body().appendElement("div").text(r.toString());
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .acl("public-read")
                .bucket(finalOutputBucket)
                .key(finalOutputKey)
                .build();

        Path path = Paths.get(System.getProperty("user.dir"), "new_tasks", "output", finalOutputKey);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path.toString()))) {
            writer.write(doc.toString());
            writer.close();
            s3.putObject(
                    putObjectRequest,
                    path
            );
        } catch (IOException e) {
            e.printStackTrace();
            severLogger.log("Failed to write the html",e);
        }
    }

    private void delete(Message m) {
        sqs.deleteMessage(
                DeleteMessageRequest.builder()
                        .queueUrl(resultsSQSUrl)
                        .receiptHandle(m.receiptHandle())
                        .build()
        );
    }

    private void consume(Message m) throws ParseException {
        store(Result.create(m));
    }

}
