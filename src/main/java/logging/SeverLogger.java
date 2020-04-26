package logging;

import java.io.IOException;
import java.util.logging.Level;

public class SeverLogger extends Logger{
    public SeverLogger(String loggerName, String fileHandlerPath) throws IOException {
        super(loggerName, fileHandlerPath, Level.SEVERE);
    }
}
