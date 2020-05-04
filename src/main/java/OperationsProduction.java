import logging.InfoLogger;
import logging.SeverLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OperationsProduction  implements Runnable{
    private final String operationsSqsName;
    private String resultsSqsName;
    private final String outputBucket;
    private Integer numOfPendingOperations;
    private ConcurrentLinkedQueue<Task.NewTask> queue;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;

    public OperationsProduction(String operationsSqsName,String resultsSqsName, String outputBucket, Integer numOfPendingOperations, ConcurrentLinkedQueue<Task.NewTask> queue, InfoLogger infoLogger, SeverLogger severLogger) {
        this.operationsSqsName = operationsSqsName;
        this.resultsSqsName = resultsSqsName;
        this.outputBucket = outputBucket;
        this.numOfPendingOperations = numOfPendingOperations;
        this.queue = queue;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
    }

    @Override
    public void run() {
        while (true){
            if(!queue.isEmpty())
                handleNewTask(queue.poll());
        }
    }

    void handleNewTask(Task.NewTask newT) {
        infoLogger.log(String.format("Handling %s", newT.getMessage().body()));
        Runtime rt = Runtime.getRuntime();
        try {
            Path pathPrefix = Paths.get("new_tasks","output");
            Process pr = rt.exec(
                    String.format(
                            "wget https://%s.s3.amazonaws.com/%s -P %s" ,
                            newT.getBucket(),
                            newT.getKeyInput(),
                            pathPrefix)
            );
            pr.waitFor();

        } catch (IOException e) {
            e.printStackTrace();
            severLogger.log("Failed handling the new task. Tries again", e);
            queue.add(newT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
