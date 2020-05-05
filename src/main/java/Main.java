import logging.InfoLogger;
import logging.SeverLogger;
import org.apache.commons.cli.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.util.Base64;
import java.util.Date;

public class Main {
    public static final String WORKER_AMI = "ami-0062816003cfcacd3";
    public static final String WORKER_TAG = "Worker";
    static SeverLogger severLogger;
    static InfoLogger infoLogger;

    public static void main(String[] args) throws IOException {
        severLogger = generateSeverLogger();
        infoLogger = generateInfoLogger();
        infoLogger.log("Start");
        Options options = new Options();
        Option inputTasksSqs = new Option("i", "tasks", true, "input tasks sqs");
        inputTasksSqs.setRequired(true);
        options.addOption(inputTasksSqs);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            severLogger.log("Failed parsing args", e);
            infoLogger.log("Exisiting");
            System.exit(1);
        }
        new Manager(
                cmd.getOptionValue("i"),
                WORKER_AMI,
                WORKER_TAG,
                infoLogger,
                severLogger
        ).serve();
        infoLogger.log("Manager done serving");
    }

    public static InfoLogger generateInfoLogger() throws IOException {
        return new InfoLogger("ManagerInfoLogger","infoLog.txt");
    }

    public static SeverLogger generateSeverLogger() throws IOException {
        return new SeverLogger("ManagerSeverLogger","severLog.txt");
    }
}
