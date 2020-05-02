import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Base64;
import java.util.List;

public class Manager {
    private String tasksSqsAddress;
    private String workerAmi;
    private String workerTag;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private SqsClient sqs;

    public Manager(String tasksSqsAddress, String workerAmi, String workerTag, InfoLogger infoLogger, SeverLogger severLogger) {
        this.tasksSqsAddress = tasksSqsAddress;
        this.workerAmi = workerAmi;
        this.workerTag = workerTag;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        ;
    }

    private void createWorker(String msg) {
        Ec2Client ec2 = Ec2Client.create();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(this.workerAmi)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .keyName("myKeyPair")
                .userData(Base64.getEncoder().encodeToString(getWorkerScript(msg).getBytes()))
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
            infoLogger.log(String.format("Successfully started EC2 instance %s based on AMI %s", instanceId, this.workerAmi));
        } catch (Ec2Exception e) {
            severLogger.log("createWorker() failed", e);
            return;
        }
    }

    private static String getWorkerScript(String arg) {
        //        String awsAccessKeyId = args[1];
//        String awsSecretAccessKey = args[2];
        return String.join("\n",
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
                String.format("java -jar theJar.jar %s", arg)
        );
    }

    public void serve() {
        String queueUrl = sqs.getQueueUrl(
                GetQueueUrlRequest.builder()
                        .queueName(this.tasksSqsAddress)
                        .build()
        ).toString();
        infoLogger.log("serving");
        while (true) {
            List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build())
                    .messages();
            if (messages.size() == 0) continue;
            Task task = toTask(messages.get(0));
            infoLogger.log(String.format("Handling %s", task.toString()));
            task.visit(this);
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(task.getMessage().receiptHandle())
                    .build());
        }
    }

}
