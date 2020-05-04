import logging.InfoLogger;
import logging.SeverLogger;
import software.amazon.awssdk.regions.Region;

public class ResultsConsumption implements Runnable{
    public ResultsConsumption(String resultsSqsName, String resultsBucket, String finalOutputBucket, String finalOutputKey, Region usEast1, InfoLogger infoLogger, SeverLogger severLogger) {
    }

    @Override
    public void run() {

    }

    public void sealConsumption() {
    }
}
