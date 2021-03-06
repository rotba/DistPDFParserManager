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
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationsProduction implements Runnable {
    private final String operationsSqsName;
    private String resultsSqsName;
    private final String resultsBucket;
    private AtomicInteger numOfPendingOperations;
    private ConcurrentLinkedQueue<Task.NewTask> queue;
    private SqsClient sqs;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private final String operationsQUrl;
    private volatile String lastGivenTimeStamp;
    private HashMap<String, HashMap<String, Integer>> operationsLeftCounter;


    public OperationsProduction(String operationsSqsName, String resultsSqsName, String resultsBucket, AtomicInteger numOfPendingOperations, ConcurrentLinkedQueue<Task.NewTask> queue, Region region, InfoLogger infoLogger, SeverLogger severLogger) {
        this.operationsSqsName = operationsSqsName;
        this.resultsSqsName = resultsSqsName;
        this.resultsBucket = resultsBucket;
        this.numOfPendingOperations = numOfPendingOperations;
        this.queue = queue;
        sqs = SqsClient.builder().region(region).build();
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        operationsQUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder().queueName(operationsSqsName).build()
        ).queueUrl();
        lastGivenTimeStamp = null;
        operationsLeftCounter = new HashMap<>();
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
            Path pathPrefix = Paths.get(System.getProperty("user.dir"), "new_tasks", "input");
            Process pr = rt.exec(
                    String.format(
                            "wget https://%s.s3.amazonaws.com/%s -P %s",
                            newT.getBucket(),
                            newT.getKeyInput(),
                            pathPrefix)
            );
            pr.waitFor();
            produceOperations(Paths.get(pathPrefix.toString(), newT.getKeyInput()).toString(), newT.getBucket(), newT.getKeyOutput());

        } catch (IOException e) {
            e.printStackTrace();
            severLogger.log("Failed handling the new task. Tries again", e);
            queue.add(newT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void produceOperations(String path, String outputBucket, String outputKey) throws IOException {
        LineIterator it = FileUtils.lineIterator(new File(path), "UTF-8");
        Integer pendingTasks = 0;
        try {
            while (it.hasNext()) {
                produceOperation(it.nextLine(), outputBucket, outputKey);
                pendingTasks++;
            }
        } finally {
            it.close();
        }
        if(!operationsLeftCounter.containsKey(outputBucket)){
            operationsLeftCounter.put(outputBucket, new HashMap<>());
        }
        operationsLeftCounter.get(outputBucket).put(outputKey, pendingTasks);
    }

    private void produceOperation(String nextLine, String outputBucket, String outputKey) {
        String[] arr = nextLine.split("\\s+");
        String timestamp = String.valueOf(new Date().getTime());
        lastGivenTimeStamp = timestamp;
        String body = String.join(" ",
                "-a", arr[0],
                "-i", arr[1],
                "-b", resultsBucket,
                "-k", timestamp,
                "-t", Instant.now().toString(),
                "-fb", outputBucket,
                "-fk", outputKey
        );
        sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(operationsQUrl)
                        .messageBody(body)
                        .build()
        );
        synchronized (numOfPendingOperations) {
            numOfPendingOperations.set(numOfPendingOperations.get() + 1);
        }
    }

    public String getLastGivenTimeStamp() {
        return lastGivenTimeStamp;
    }

    public synchronized Integer numberOfPendingOperationsLeft(String b, String k) {
        Integer integer = operationsLeftCounter.get(b).get(k);
        infoLogger.log(String.format("Number of pending tasks for %s %s is %d",b,  k, integer));
        return integer;
    }

    public synchronized void operationFulfilled(String b, String k) {
        infoLogger.log(String.format("Decrementing number of pending tasks for %s %s", b, k));
        operationsLeftCounter.get(b).replace(
                k,
                operationsLeftCounter.get(b).get(k) - 1
        );
        infoLogger.log(String.format("And now the number of pending tasks for %s %s is %d",b,  k, operationsLeftCounter.get(b).get(k)));
    }
}
