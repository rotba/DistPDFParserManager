import org.apache.commons.cli.*;
import software.amazon.awssdk.services.sqs.model.Message;

public abstract class Task {
    private Message message;

    public Task(Message m) {
        this.message = m;
    }

    public static Task fromMessage(Message m) throws ParseException {
        Options options = new Options();
        Option tasksSqs = new Option("t", "task", true, "the task");
        tasksSqs.setRequired(true);
        options.addOption(tasksSqs);
        Option bucket = new Option("b", "bucket", true, "the bucket of the input file");
        bucket.setRequired(true);
        options.addOption(bucket);
        Option key = new Option("k", "key", true, "the key of the input file");
        key.setRequired(true);
        options.addOption(key);
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
                    cmd.getOptionValue("k"),
                    m
            );
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

        private final String key;

        public NewTask(String bucket, String key, Message m) {
            super(m);
            this.bucket = bucket;
            this.key = key;
        }

        public String getBucket() {
            return bucket;
        }

        public String getKey() {
            return key;
        }

        @Override
        public void visit(Manager manager) {
            manager.accept(this);
        }


    }
}
