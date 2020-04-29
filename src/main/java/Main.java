import logging.InfoLogger;
import logging.SeverLogger;

import java.io.IOException;

public class Main {
    public static final String TASKE_QUEUE_NAME = "rotemb271ManagerTasksQ";
    public static void main(String[] args) throws IOException {
        SeverLogger severLogger = new SeverLogger("ManagerSeverLogger","severLog.txt");
        InfoLogger infoLogger = new InfoLogger("ManagerInfoLogger","infoLog.txt");
        infoLogger.log("Start");
        while (true);
    }
}
