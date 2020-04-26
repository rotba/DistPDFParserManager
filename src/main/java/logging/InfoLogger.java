package logging;

import java.io.IOException;
import java.util.logging.Level;

public class InfoLogger  extends Logger{
    public InfoLogger(String loggerName, String fileHandlerPath) throws IOException {
        super(loggerName, fileHandlerPath, Level.INFO);
    }

}
