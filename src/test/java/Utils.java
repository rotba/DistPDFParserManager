import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

public class Utils {
    public static void waitDispatchWorker() {
        try {
            Thread.sleep(5*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitCreateInstances() {
        try {
            Thread.sleep(5*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    static boolean htmlContains(File download, String s) {
        try {
            Document doc = Jsoup.parse(
                    download,
                    "utf-8"
            );
            String text = doc.body().text();
            return text.contains(s);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    static File download(String outputBucket, String outputKey) {
        Runtime rt = Runtime.getRuntime();
        try {
            String address = String.format("https://%s.s3.amazonaws.com/%s", outputBucket, outputKey);
            Path pathPrefix = Paths.get(System.getProperty("user.dir"), "test_files", "output");
            Process pr = rt.exec(String.format("wget %s -P %s", address, pathPrefix));
            pr.waitFor();
            return new File(Paths.get(pathPrefix.toString(), outputKey).toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        fail();
        return null;
    }
}
