import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.cli.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {
    private final ResultsConsumption resultsConsumption;
    private String operationsSqsName;
    private String tasksSqsName;
    private String tasksQueueUrl;
    private String operationsResultsSqsName;
    private String operationsBucket;
    private static  String staticOperationsBucket;
    private String workerAmi;
    private String workerTag;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private SqsClient sqs;
    private S3Client s3;
    private ConcurrentLinkedQueue<Task.NewTask> newTasks;
    private Integer workingInstances;
    private AtomicInteger numberOfPendingTasks;
    private Thread operationsProducer;
    private Thread operationsResultsConsumer;
    private Thread instancesBalancer;

    public Manager(String tasksSqsName, String kid, String sak, String workerAmi, String workerTag, InfoLogger infoLogger, SeverLogger severLogger) {
        this.tasksSqsName = tasksSqsName;
        this.workerAmi = workerAmi;
        this.workerTag = workerTag;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        s3 = S3Client.create();
        tasksQueueUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder()
                        .queueName(tasksSqsName)
                        .build()
        ).queueUrl();
        newTasks = new ConcurrentLinkedQueue();
        numberOfPendingTasks = new AtomicInteger(0);
        workingInstances = 0;
        operationsSqsName = "rotemb271Operations" + new Date().getTime();
        operationsResultsSqsName = "rotemb271OperationResults" + new Date().getTime();
        operationsBucket = "rotemb271-operations-results-bucket" + new Date().getTime();
        staticOperationsBucket = operationsBucket;
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(operationsSqsName)
                        .build()
        );
        sqs.createQueue(
                CreateQueueRequest.builder()
                        .queueName(operationsResultsSqsName)
                        .build()
        );
        s3.createBucket(
                CreateBucketRequest.builder()
                        .bucket(operationsBucket)
                        .build()
        );
        OperationsProduction operationsProduction = new OperationsProduction(
                operationsSqsName,
                operationsResultsSqsName,
                operationsBucket,
                numberOfPendingTasks,
                newTasks,
                Region.US_EAST_1,
                infoLogger,
                severLogger
        );
        operationsProducer = new Thread(operationsProduction);
        instancesBalancer = new Thread(new InstancesBalancing(
                numberOfPendingTasks,
                operationsSqsName,
                operationsResultsSqsName,
                Region.US_EAST_1,
                workerAmi,
                kid,
                sak,
                UserDataStrategy.NON_INITIAL,
                infoLogger,
                severLogger
        ));
        resultsConsumption = new ResultsConsumption(
                operationsResultsSqsName,
                operationsBucket,
                numberOfPendingTasks,
                Region.US_EAST_1,
                new PendingTasksSocket() {
                    @Override
                    public Integer numberOfPendingOperationsLeft(String b, String k) {
                        return operationsProduction.numberOfPendingOperationsLeft(b,k);
                    }

                    @Override
                    public void operationFulfilled(String b, String k) {
                        operationsProduction.operationFulfilled(b,k);
                    }
                },
                infoLogger,
                severLogger

        );
        operationsResultsConsumer = new Thread(resultsConsumption);
        operationsProducer.start();
        instancesBalancer.start();
        operationsResultsConsumer.start();
    }


    public static String FORTETSINGgetOperationsBucket() {
        return staticOperationsBucket;
    }


//    private void createWorker(String msg) {
//        Ec2Client ec2 = Ec2Client.create();
//        RunInstancesRequest runRequest = RunInstancesRequest.builder()
//                .imageId(this.workerAmi)
//                .instanceType(InstanceType.T2_MICRO)
//                .maxCount(1)
//                .minCount(1)
//                .keyName("myKeyPair")
//                .userData(Base64.getEncoder().encodeToString(getWorkerScript(msg).getBytes()))
//                .build();
//
//        RunInstancesResponse response = ec2.runInstances(runRequest);
//        String instanceId = response.instances().get(0).instanceId();
//        Tag tag = Tag.builder()
//                .key("Name")
//                .value(this.workerTag)
//                .build();
//
//        CreateTagsRequest tagsRequest = CreateTagsRequest.builder()
//                .resources(instanceId)
//                .tags(tag)
//                .build();
//
//        try {
//            ec2.createTags(tagsRequest);
//            infoLogger.log(String.format("Successfully started EC2 instance %s based on AMI %s", instanceId, this.workerAmi));
//        } catch (Ec2Exception e) {
//            severLogger.log("createWorker() failed", e);
//            return;
//        }
//    }


    private Task getNextTask() throws ParseException, Task.NotImplementedException, InterruptedException {
        infoLogger.log("Waiting for next task");
        Message m = null;
        while (m == null) {
            List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(tasksQueueUrl)
                    .maxNumberOfMessages(1)
                    .build())
                    .messages();
            if (messages.size() == 0) continue;
            else m = messages.get(0);
        }
        infoLogger.log(String.format("Got message: %s", m.body()));
        return Task.fromMessage(m);
    }

    private void delete(Task task) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(tasksQueueUrl)
                .receiptHandle(task.getMessage().receiptHandle())
                .build());
    }

    public void accept(Task.NewTask newTask) {
        newTasks.add(newTask);
    }

    public void accept(Task.Terminate terminate) {
        terminate();
    }

    public void terminate() {
        infoLogger.log("terminating");
        operationsResultsConsumer.interrupt();
        operationsResultsConsumer.interrupt();
        instancesBalancer.interrupt();
    }

    public void serve() {
        infoLogger.log("serving");

        while (true) {
            try {
                Task task = getNextTask();
                infoLogger.log(String.format("Handling %s", task.toString()));
                task.visit(this);
                delete(task);
            } catch (ParseException | Task.NotImplementedException e) {
                severLogger.log("Parsing problem, probably failed parsing the message", e);
            }catch (InterruptedException e){
                terminate();
                return;
            }
        }
    }
    interface PendingTasksSocket{
        Integer numberOfPendingOperationsLeft(String b, String k);
        void operationFulfilled(String b, String k);
    }
}
