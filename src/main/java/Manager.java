import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Base64;

public class Manager {
    private String tasksSqsAddress;
    private String workerAmi;
    private String workerTag;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private SqsClient tasksSqs;

    public Manager(String tasksSqsAddress,String workerAmi ,String workerTag, InfoLogger infoLogger, SeverLogger severLogger) {
        this.tasksSqsAddress = tasksSqsAddress;
        this.workerAmi = workerAmi;
        this.workerTag = workerTag;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        tasksSqs = SqsClient.builder().region(Region.US_EAST_1).build();;
    }

    public void serve(){
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(this.tasksSqsAddress)
                .build();
        String queueUrl = tasksSqs.getQueueUrl(getQueueUrlRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        while (true){
            for (Message m : tasksSqs.receiveMessage(receiveRequest).messages()) {
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(m.receiptHandle())
                        .build();
                infoLogger.log(String.format("Got message: %s\nCreating a worker to handle the message", m.toString()));
                System.out.println("The message: "+m.toString());
                createWorker(m.toString());
                tasksSqs.deleteMessage(deleteRequest);
            }
        }
    }

    private void createWorker(String toString) {
        Ec2Client ec2 = Ec2Client.create();
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
                .imageId(this.workerAmi)
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
                .value(this.workerTag)
                .build();

        CreateTagsRequest tagsRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagsRequest);
            infoLogger.log(String.format("Successfully started EC2 instance %s based on AMI %s",instanceId, this.workerAmi));
        }catch (Ec2Exception e){
            severLogger.log("createWorker() failed", e);
            return;
        }
    }
}
