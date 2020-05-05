import org.apache.commons.cli.*;
import software.amazon.awssdk.services.sqs.model.Message;

abstract class Result {
    protected String operation;
    protected String inputFile;
    private final String outputBucket;

    private final String outputKey;

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
        Option outputBucket = new Option("b", "bucket", true, "output bucket");
        outputBucket.setRequired(true);
        operationParsingOptions.addOption(outputBucket);
        Option outputKey = new Option("k", "key", true, "output key");
        outputKey.setRequired(true);
        operationParsingOptions.addOption(outputKey);
        CommandLineParser operationParser = new DefaultParser();
        CommandLine operation = operationParser.parse(operationParsingOptions, m.body().split("\\s+"));
        if (operation.getOptionValue("s").equals("SUCCESS")) {
            return new SuccessfulResult(
                    operation.getOptionValue("a"),
                    operation.getOptionValue("i"),
                    operation.getOptionValue("b"),
                    operation.getOptionValue("k"),
                    operation.getOptionValue("u")

            );
        } else {
            return new FailResult(
                    operation.getOptionValue("a"),
                    operation.getOptionValue("i"),
                    operation.getOptionValue("b"),
                    operation.getOptionValue("k"),
                    operation.getOptionValue("d")
            );
        }
    }

    protected Result(String operation, String inputFile, String outputBucket, String outputKey) {
        this.operation = operation;
        this.inputFile = inputFile;
        this.outputBucket = outputBucket;
        this.outputKey = outputKey;
    }

    public String getOutputBucket() {
        return outputBucket;
    }

    public String getOutputKey() {
        return outputKey;
    }

    private static class SuccessfulResult extends Result {
        private String outputFile;

        public SuccessfulResult(String operation, String inputFile, String outputBucket, String outputKey, String outputFile) {
            super(operation, inputFile, outputBucket, outputKey);
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

        public FailResult(String operation, String inputFile, String outputBucket, String outputKey, String failureDesc) {
            super(operation, inputFile,outputBucket, outputKey);
            this.failure = failureDesc;
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
