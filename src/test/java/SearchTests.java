/**
 * Created by jcatrambone on 6/18/17.
 */
import kotlin.Pair;
import org.junit.*;
import io.xoana.beautifulday.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;

public class SearchTests {
	private final int NUM_POINTS = 100000;
	private final int NUM_WORKERS = 4;
	private final int DIMENSIONS = 500;
	private final Random random = new Random();

	BDMaster master = null;
	Thread[] workerThreads;

	private DataPoint makeRandomPoint(int id) {
		float[] data = new float[DIMENSIONS];
		for(int i = 0; i < DIMENSIONS; i++) {
			data[i] = (float)random.nextGaussian();
		}
		return new DataPoint(id, data);
	}

	public void setup() {
		System.out.println("Starting master thread.");
		master = new BDMaster(4567);
		master.main();

		workerThreads = new Thread[NUM_WORKERS];
		for(int i=0; i < NUM_WORKERS; i++) {
			System.out.println("Starting worker thread " + i + ".");
			Thread workerThread = new Thread(() -> {
				BDWorker worker = new BDWorker(new InetSocketAddress(InetAddress.getLoopbackAddress(), 4567));
				worker.main();
			});
			workerThread.setName("BDWorker" + i);
			workerThread.start();
			workerThreads[i] = workerThread;
		}

		while(master.getNumWorkers() < NUM_WORKERS) {
			System.out.println("Waiting for workers to register.");
			try {
				Thread.sleep(1000);
			} catch(InterruptedException ie) {
			}
		}
	}

	public void teardown() {
		master.shutdown();
	}

	@Test
	public void networkTest() {
		setup();
		System.out.println("Adding "+ NUM_POINTS +" points to our nodes.");
		for(int i=0; i < NUM_POINTS; i++) {
			master.addPoint(makeRandomPoint(i));
			if((i+1)%1000 == 0) {
				System.out.println((i*100 / NUM_POINTS) + "% complete.");
			}
		}

		System.out.println("Waiting (briefly) for workers to get the data.");
		try { Thread.sleep(1000); } catch(InterruptedException ie) {}

		for(int i=0; i < 1000; i++) {
			System.out.println("Querying master for point.  Test " + i);
			long startTime = System.currentTimeMillis();
			int qid = master.submitQuery(makeRandomPoint(5), DistanceMetric.EUCLIDEAN, 3);
			Pair<Integer, Float>[] results = null;
			while (results == null) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException ie) {
				}
				results = master.getQueryResults(qid);
			}
			System.out.println("Got results.");
			long endTime = System.currentTimeMillis();
			System.out.println("Query time: " + (endTime - startTime));
		}
		teardown();
	}

	@Test
	public void testSearchStoresResults() {
		// Randomly generate ten distance results.
		int[] ids = new int[10];
		float[] distances = new float[10];
		Random random = new Random();
		for(int i=0; i < 10; i++) {
			ids[i] = random.nextInt(100);
			distances[i] = random.nextFloat()*100;
		}
		// Add all to the result set.  Should have the top five.
		ResultSet res = new ResultSet(new Query(1, 5, null, null));
		for(int i=0; i < 10; i++) {
			res.addResultToSet(ids[i], distances[i]);
		}
		// Fetch them.  Should be in sorted order.
		float[] resultDistances = res.getResultDistances();
		for(int i=0; i < 5; i++) {
			// This should be less than all greater distances in the result set.
			for(int j=i; j < 5; j++) {
				Assert.assertTrue(resultDistances[i] <= resultDistances[j]);
			}
			// And should be less than 10-(i+1) of the other items.
			int countOfElementsGreaterThanThis = 0;
			for(int j=0; j < 10; j++) {
				if(distances[j] >= resultDistances[i]) {
					countOfElementsGreaterThanThis++;
				}
			}
			// Could say == i+1, but if we happen to have two that are exactly the same...
			Assert.assertTrue(countOfElementsGreaterThanThis > i+1);
		}
	}
}
