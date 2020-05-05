import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InstancesBalancing implements Runnable {
    public static final Integer LOAD_FACTOR = 100;
    private final AtomicInteger pendingTasks;
    private final String operationsSqs;
    private final String notificationsSqs;
    private final String workerAmi;
    private final InfoLogger infoLogger;
    private final SeverLogger severLogger;
    private static int n;
    private Ec2Client ec2Client;
    private AtomicInteger workingInstances;

    public InstancesBalancing(AtomicInteger pendingTasks, String operationsSqs , String notificationsSqs, Region region, String workerAmi, InfoLogger infoLogger, SeverLogger severLogger) {
        this.pendingTasks = pendingTasks;
        this.notificationsSqs = notificationsSqs;
        this.workerAmi = workerAmi;
        this.infoLogger = infoLogger;
        this.severLogger = severLogger;
        n = LOAD_FACTOR;
        ec2Client = Ec2Client.builder().region(region).build();
        this.operationsSqs = operationsSqs;
        workingInstances = new AtomicInteger(0);
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
                .userData(Base64.getEncoder().encodeToString(getWorkerScript(args).getBytes()))
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
                if(isWorker(instance) && isActive(instance))
                    ans.add(instance);
            }
        }
        return ans;
    }

    private boolean isActive(Instance instance) {
        return instance.state().name().equals(InstanceStateName.RUNNING) ||
                instance.state().name().equals(InstanceStateName.PENDING) ;
    }

    int checkNumberOfWorkers() {
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
            num--;
            if(num==0) break;
        }
    }

    @Override
    public void run() {
        deleteWorkers(100);
        AtomicInteger pendingTasksSnapShot  = new AtomicInteger(0);
        while (true){
            try {
                workingInstances.set(checkNumberOfWorkers());
                synchronized (this.pendingTasks) {
                    pendingTasksSnapShot.set(this.pendingTasks.intValue());
                }
                if (workingInstances.get() == 0 && pendingTasksSnapShot.get() == 0) {
                    continue;
                } else if (workingInstances.get() == 0 && pendingTasksSnapShot.get() > 0) {
                    createWorkers(1);
                    workingInstances.set(1);
                } else if (pendingTasksSnapShot.get() / workingInstances.get() > n) {
                    int num = pendingTasksSnapShot.get() / n - workingInstances.get();
                    createWorkers(num);
                    workingInstances.addAndGet(num);
                } else if (workingInstances.get() > 1 && pendingTasksSnapShot.get() / workingInstances.get() < n) {
                    int num = workingInstances.get() - pendingTasksSnapShot.get() / n;
                    deleteWorkers(num);
                    workingInstances.addAndGet(-num);
                }
            }catch (SdkClientException e){
                severLogger.log("InstancesBalancing - Unexpected", e);
                return;
            }catch (Exception e){
                severLogger.log("InstancesBalancing - Unexpected", e);
                return;
            }
        }
    }

    int forTestingGetNumOfInstances(){
        return workingInstances.get();
    }
}
