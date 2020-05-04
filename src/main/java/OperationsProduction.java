import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OperationsProduction implements Runnable {
    private final String operationsSqsName;
    private String resultsSqsName;
    private final String outputBucket;
    private Integer numOfPendingOperations;
    private ConcurrentLinkedQueue<Task.NewTask> queue;
    private SqsClient sqs;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private final String operationsQUrl;

    public OperationsProduction(String operationsSqsName, String resultsSqsName, String outputBucket, Integer numOfPendingOperations, ConcurrentLinkedQueue<Task.NewTask> queue, Region region, InfoLogger infoLogger, SeverLogger severLogger) {
        this.operationsSqsName = operationsSqsName;
        this.resultsSqsName = resultsSqsName;
        this.outputBucket = outputBucket;
        this.numOfPendingOperations = numOfPendingOperations;
        this.queue = queue;
        sqs = SqsClient.builder().region(region).build();
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        operationsQUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder().queueName(operationsSqsName).build()
        ).queueUrl();
    }

    @Override
    public void run() {
        while (true) {
            if (!queue.isEmpty())
                handleNewTask(queue.poll());
        }
    }

    void handleNewTask(Task.NewTask newT) {
        infoLogger.log(String.format("Handling %s", newT.getMessage().body()));
        Runtime rt = Runtime.getRuntime();
        try {
            Path pathPrefix = Paths.get(System.getProperty("user.dir"),"new_tasks", "input");
            Process pr = rt.exec(
                    String.format(
                            "wget https://%s.s3.amazonaws.com/%s -P %s",
                            newT.getBucket(),
                            newT.getKeyInput(),
                            pathPrefix)
            );
            pr.waitFor();
            produceOperations(Paths.get(pathPrefix.toString(), newT.getKeyInput()).toString());

        } catch (IOException e) {
            e.printStackTrace();
            severLogger.log("Failed handling the new task. Tries again", e);
            queue.add(newT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void produceOperations(String path) throws IOException {
        LineIterator it = FileUtils.lineIterator(new File(path), "UTF-8");
        try {
            while (it.hasNext()) {
                produceOperation(it.nextLine());
            }
        } finally {
            it.close();
        }
    }

    private void produceOperation(String nextLine) {
        String[] arr = nextLine.split("\\s+");
        String body = String.join(" ",
                "-a", arr[0],
                "-i", arr[1],
                "-b", outputBucket,
                "-k", arr[1],
                "-t", Instant.now().toString()
        );
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(operationsQUrl)
                        .messageBody(body)
                        .build()
        );
    }
}
