import logging.InfoLogger;
import logging.SeverLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;

public class OperationsProduction  implements Runnable{
    private String resultsSqsName;
    private Integer numOfPendingOperations;
    private ConcurrentLinkedQueue<Task.NewTask> queue;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;

    public OperationsProduction(String resultsSqsName, Integer numOfPendingOperations, ConcurrentLinkedQueue<Task.NewTask> queue, InfoLogger infoLogger, SeverLogger severLogger) {
        this.resultsSqsName = resultsSqsName;
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

    private void handleNewTask(Task.NewTask newT) {
        infoLogger.log(String.format("Handling %s", newT.getMessage().body()));
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(String.format("wget %s -P %s" ,address, Paths.get("new_tasks","output")));
            pr.waitFor();
            return new File(Paths.get(System.getProperty("user.dir"),key).toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
