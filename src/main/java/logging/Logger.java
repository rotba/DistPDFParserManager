package logging;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

public abstract class Logger {
    private String loggerName;
    private FileHandler fileHandler;
    protected Level level;
    protected java.util.logging.Logger logger;

    public Logger(String loggerName, String fileHandlerPath, Level level) throws IOException {
        this.loggerName = loggerName;
        this.fileHandler = new FileHandler(fileHandlerPath);
        this.level = level;
        this.fileHandler.setFormatter(new SimpleFormatter());
        logger = java.util.logging.Logger.getLogger(loggerName);
        logger.setLevel(this.level);
        logger.addHandler(this.fileHandler);
    }
    public void log(String msg){
        logger.log(level,msg);
    }
    public void log(String msg, Throwable throwable){
        logger.log(level,throwable.getStackTrace().toString(),throwable);
    }
}


