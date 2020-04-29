import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.util.Base64;

public class Main {
    public static final String TASKE_QUEUE_NAME = "rotemb271ManagerTasksQ";
    private static final String WORKER_AMI = "ami-0062816003cfcacd3";
    public static final String WORKER_TAG = "Worker";
    private static SeverLogger severLogger;
    private static InfoLogger infoLogger;

    public static void main(String[] args) throws IOException {
        severLogger = new SeverLogger("ManagerSeverLogger","severLog.txt");
        infoLogger = new InfoLogger("ManagerInfoLogger","infoLog.txt");
        infoLogger.log("Start");
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        CreateQueueRequest request  = CreateQueueRequest.builder()
                .queueName(TASKE_QUEUE_NAME)
                .build();

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(TASKE_QUEUE_NAME)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        while (true){
            for (Message m : sqs.receiveMessage(receiveRequest).messages()) {
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(m.receiptHandle())
                        .build();
                infoLogger.log(String.format("Got message: %s\nCreating a worker to handle the message", m.toString()));
                System.out.println("The message: "+m.toString());
                createWorker(m.toString());
                sqs.deleteMessage(deleteRequest);
            }
        }
    }

    private static void createWorker(String toString) {
        Ec2Client ec2 = Ec2Client.create();
        String workerAMI = WORKER_AMI;
//        String awsAccessKeyId = args[1];
//        String awsSecretAccessKey = args[2];
        String script = String.join("\n",
                "#!/bin/bash",
                "set -e -x",
//                String.format("aws configure set aws_access_key_id %s", awsAccessKeyId),
//                String.format("aws configure set aws_secret_access_key %s", awsSecretAccessKey),
                "cd ..",
                "cd /home/ec2-user",
                "if [ -d \"DistPDFParser\" ]; then echo \"Repo exists\" ;",
                "else",
                "git clone https://github.com/rotba/DistPDFParser.git",
                "cd DistPDFParser",
                "git submodule init",
                "git submodule update worker;",
                "fi",
                "cd worker",
                "git pull origin master",
                "mvn install",
                "cd target",
                "java -jar theJar.jar"
        );
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(workerAMI)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .keyName("myKeyPair")
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Name")
                .value(WORKER_TAG)
                .build();

        CreateTagsRequest tagsRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagsRequest);
            infoLogger.log(String.format("Successfully started EC2 instance %s based on AMI %s",instanceId, workerAMI));
        }catch (Ec2Exception e){
            severLogger.log("createWorker() failed", e);
            return;
        }
    }
}
