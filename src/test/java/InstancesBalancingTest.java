
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class InstancesBalancingTest extends MainTest {


    private Thread theOutThread;
    private InstancesBalancing out;
    private AtomicInteger pendingTasks;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        pendingTasks = new AtomicInteger(0);
        out = new InstancesBalancing(
                pendingTasks,
                operationSqsName,
                resultsSqsName,
                Region.US_EAST_1,
                Main.WORKER_AMI,
                IGNOREMECREDENTIALS.kid,
                IGNOREMECREDENTIALS.sak,
                UserDataStrategy.INITIAL,
                Main.generateInfoLogger(),
                Main.generateSeverLogger()
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        theOutThread.interrupt();
    }

    private int countWorkers() {
        return out.checkNumberOfWorkers();
    }

    @Test
    public void testBalancingCorrectly() throws IOException, InterruptedException {
        theOutThread = new Thread(out);
        pendingTasks.set(InstancesBalancing.LOAD_FACTOR * 2);
        theOutThread.start();
        while (out.forTestingGetNumOfInstances() < 2) {
        }
        Thread.sleep(5*1000);
        assertEquals(2, countWorkers());
        synchronized (pendingTasks){
            pendingTasks.set(InstancesBalancing.LOAD_FACTOR * 1);
        }
        while (out.forTestingGetNumOfInstances() > 1) {
        }
        Thread.sleep(5*1000);
        assertEquals(1, countWorkers());
    }
}