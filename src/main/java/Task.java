import org.apache.commons.cli.*;
import software.amazon.awssdk.services.sqs.model.Message;

public abstract class Task {
    private Message message;

    public Task(Message m) {
        this.message = m;
    }

    public static Task fromMessage(Message m) throws ParseException, NotImplementedException {
        Options options = new Options();
        Option tasksSqs = new Option("t", "task", true, "the task");
        tasksSqs.setRequired(true);
        options.addOption(tasksSqs);
        Option bucket = new Option("b", "bucket", true, "the bucket of the input file");
        bucket.setRequired(true);
        options.addOption(bucket);
        Option keyInput= new Option("ki", "keyInput", true, "the key of the input file");
        keyInput.setRequired(true);
        options.addOption(keyInput);
        Option keyOutput= new Option("ko", "keyOutput", true, "the key of the output file");
        keyOutput.setRequired(true);
        options.addOption(keyOutput);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, m.body().split("\\s+"));
        } catch (ParseException e) {
            throw e;
        }
        if(cmd.getOptionValue("t").equals("new task")){
            return new NewTask(
                    cmd.getOptionValue("b"),
                    cmd.getOptionValue("ki"),
                    cmd.getOptionValue("ko"),
                    m
            );
        }else{
            throw new NotImplementedException();
        }
    }

    public abstract void visit(Manager manager);

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public  static class NewTask extends Task {
        private final String bucket;

        private final String keyInput;
        private final String keyOutput;

        public String getKeyOutput() {
            return keyOutput;
        }

        public NewTask(String bucket, String keyInput, String keyOutput, Message m) {
            super(m);
            this.bucket = bucket;
            this.keyInput = keyInput;
            this.keyOutput = keyOutput;
        }

        public String getBucket() {
            return bucket;
        }

        public String getKeyInput() {
            return keyInput;
        }

        @Override
        public void visit(Manager manager) {
            manager.accept(this);
        }


    }

    public static class NotImplementedException extends Throwable {
    }
}
