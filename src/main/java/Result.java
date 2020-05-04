import org.apache.commons.cli.*;
import software.amazon.awssdk.services.sqs.model.Message;

abstract class Result {
    protected String operation;
    protected String inputFile;

    public static Result create(Message m) throws ParseException {
        Options operationParsingOptions = new Options();
        Option action = new Option("a", "action", true, "action");
        action.setRequired(true);
        operationParsingOptions.addOption(action);
        Option input = new Option("i", "input", true, "input file");
        input.setRequired(true);
        operationParsingOptions.addOption(input);
        Option status = new Option("s", "status", true, "status");
        status.setRequired(true);
        operationParsingOptions.addOption(status);
        Option url = new Option("u", "url", true, "result url");
        url.setRequired(true);
        operationParsingOptions.addOption(url);
        Option timestamp = new Option("t", "timestamp", true, "timestamp");
        timestamp.setRequired(true);
        operationParsingOptions.addOption(timestamp);
        Option description = new Option("d", "description", true, "description");
        description.setRequired(true);
        operationParsingOptions.addOption(description);
        CommandLineParser operationParser = new DefaultParser();
        CommandLine operation = operationParser.parse(operationParsingOptions, m.body().split("\\s+"));
        if (operation.getOptionValue("s").equals("SUCCESS")) {
            return new SuccessfulResult(
                    operation.getOptionValue("a"),
                    operation.getOptionValue("i"),
                    operation.getOptionValue("u")
            );
        }else{
            return new FailResult(
                    operation.getOptionValue("a"),
                    operation.getOptionValue("i"),
                    operation.getOptionValue("d")
            );
        }
    }

    protected Result(String operation, String inputFile) {
        this.operation = operation;
        this.inputFile = inputFile;
    }

    private static class SuccessfulResult extends Result {
        private String outputFile;

        public SuccessfulResult(String operation, String inputFile, String outputFile) {
            super(operation, inputFile);
            this.outputFile = outputFile;
        }

        @Override
        public String toString() {
            return String.join(" ",
                    operation,
                    inputFile,
                    outputFile
            );
        }
    }

    private static class FailResult extends Result {
        private String failure;

        public FailResult(String operation, String inputFile, String failureDesc) {
            super(operation, inputFile);
            this.failure = failure;
        }
        @Override
        public String toString() {
            return String.join(" ",
                    operation,
                    inputFile,
                    failure
            );
        }
    }
}
