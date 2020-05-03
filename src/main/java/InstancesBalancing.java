import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class InstancesBalancing implements Runnable {
    private final Integer pendingTask;
    private final String operationsSqs;
    private final String notificationsSqs;
    private final String workerAmi;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private Integer pendingTasks;
    private static int n;
    private Ec2Client ec2Client;

    public InstancesBalancing(Integer pendingTask,String operationsSqs ,String notificationsSqs, Region region, String workerAmi, InfoLogger infoLogger, SeverLogger severLogger) {
        this.pendingTask = pendingTask;
        this.notificationsSqs = notificationsSqs;
        this.workerAmi = workerAmi;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        this.pendingTasks = pendingTasks;
        n = 100;
        ec2Client = Ec2Client.builder().region(region).build();
        this.operationsSqs = operationsSqs;
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

    private void createWorkers(int num) {
        infoLogger.log(String.format("Creating %d instances", num));
        String args = String.join(" ",
                "-n", notificationsSqs,
                "-o",operationsSqs
        );
        RunInstancesResponse response = ec2Client.runInstances(RunInstancesRequest.builder()
                .imageId(workerAmi)
                .instanceType(InstanceType.T2_MICRO)
                .keyName("myKeyPair")
                .userData(getWorkerScript(args))
                .maxCount(num)
                .minCount(num)
                .build());
        for (Instance instance :response.instances()) {
            Tag tag = Tag.builder()
                    .key("Name")
                    .value("Worker")
                    .build();
            ec2Client.createTags(
                    CreateTagsRequest.builder()
                    .tags(tag)
                    .resources(instance.instanceId())
                    .build()
            );
        }
    }

    private List<Instance> getWorkers() {
        List<Instance> ans  =new ArrayList<>();
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();
        for (Reservation reservation:
        ec2Client.describeInstances(describeInstancesRequest).reservations()) {
            for (Instance instance:
                 reservation.instances()) {
                if(isWorker(instance))
                    ans.add(instance);
            }
        }
        return ans;
    }
    private int checkNumberOfWorkers() {
        return getWorkers().size();
    }

    private boolean isWorker(Instance instance) {
        for (Tag t : instance.tags() ){
            if(t.key().equals("Name") && t.value().equals("Worker")){
                return true;
            }
        }
        return false;
    }

    private void deleteWorkers(int num) {
        infoLogger.log(String.format("Terminating %d instances", num));
        if (num <= 0 ) return;
        List<Instance> workers = getWorkers();
        for (Instance worker:workers) {
            ec2Client.terminateInstances(
                    TerminateInstancesRequest.builder()
                            .instanceIds(worker.instanceId())
                            .build()
            );
            num++;
            if(num==0) break;
        }
    }

    @Override
    public void run() {
        while (true){
            try {
                int workingInstances = checkNumberOfWorkers();
                int pendingTasksSnapShot;
                synchronized (pendingTasks){
                    pendingTasksSnapShot=pendingTasks;
                }
                if(workingInstances==0 && pendingTasksSnapShot ==0){
                    continue;
                }else if(workingInstances==0 && pendingTasksSnapShot >0){
                    createWorkers(1);
                }else if(pendingTasksSnapShot / workingInstances > n){
                    createWorkers(pendingTasksSnapShot/n - workingInstances);
                }else if(pendingTasksSnapShot / workingInstances < n){
                    deleteWorkers(workingInstances - pendingTasksSnapShot/n );
                }
            }catch (Exception e){
                severLogger.log("InstancesBalancing - Unexpected", e);
            }
        }
    }
}
