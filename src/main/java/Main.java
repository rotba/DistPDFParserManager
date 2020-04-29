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
    static SeverLogger severLogger;
    static InfoLogger infoLogger;

    public static void main(String[] args) throws IOException {
        severLogger = new SeverLogger("ManagerSeverLogger","severLog.txt");
        infoLogger = new InfoLogger("ManagerInfoLogger","infoLog.txt");
        infoLogger.log("Start");
        new Manager(
                TASKE_QUEUE_NAME,
                WORKER_AMI,
                WORKER_TAG,
                infoLogger,
                severLogger
        ).serve();
    }
}
