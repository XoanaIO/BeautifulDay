/**
 * Created by jcatrambone on 6/16/17.
 */

import org.junit.Test;
import org.junit.Assert;
import org.junit.Assume;
import io.xoana.beautifulday.*;

import java.util.Random;

public class DistanceMetricTests {

	@Test
	public void verifyMetricSpace() {
		// The distance metrics should define a metric space:
		// d(x,y) >= 0
		// d(x,y) = 0 -> x == y
		// d(x,y) == d(y,x)
		// d(x,z) <= d(x,y) + d(y,z)

		// Testing protocol:
		// Test the dense set from 1-10, then 100, 200, 500, 1000.
		// Randomly generate 3 points over n tests.
		final Random random = new Random();
		final int NUM_TESTS = 10;
		for(DistanceMetric metric : DistanceMetric.values()) {
			System.out.println("Testing metric " + metric.name());
			for (int dimensions : new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 200, 500, 1000}) {
				// Populate data.
				float[] xData = new float[dimensions];
				float[] yData = new float[dimensions];
				float[] zData = new float[dimensions];
				for (int i = 0; i < dimensions; i++) {
					xData[i] = (float) random.nextGaussian();
					yData[i] = (float) random.nextGaussian();
					zData[i] = (float) random.nextGaussian();
				}
				// Create points.
				DataPoint x = new DataPoint(1, xData);
				DataPoint y = new DataPoint(2, yData);
				DataPoint z = new DataPoint(3, zData);

				// Verify all the points hold.
				// d(x,y) > 0
				Assert.assertTrue(metric.calculate(x, y) >= 0);
				// d(x,y) == 0 -> x == y
				// d(x,y) == d(y,x)
				Assert.assertTrue(metric.calculate(x,y) == metric.calculate(y, x));
				// d(x,z) <= d(x,y) + d(y,z)
				Assert.assertTrue(metric.calculate(x,z) <= metric.calculate(x,y)+metric.calculate(y,z));
			}
		}
	}

	@Test
	public void testEuclidean() {
		DataPoint p1 = new DataPoint(1, new float[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
		DataPoint p2 = new DataPoint(2, new float[]{0, 1, 0, 0, 0, 0, 0, 0, 0, 0});
		Assert.assertEquals(1.0f, DistanceMetric.EUCLIDEAN.calculate(p1, p2), 1.0e-8);
		Assert.assertEquals(0.0f, DistanceMetric.EUCLIDEAN.calculate(p2, p2), 1.0e-8);

		p1 = new DataPoint(1, new float[]{1, 0});
		p2 = new DataPoint(2, new float[]{0, 1});
		Assert.assertEquals(Math.sqrt(2.0), DistanceMetric.EUCLIDEAN.calculate(p1, p2), 1.0e-8);

		p1 = new DataPoint(1, new float[]{3, 0});
		p2 = new DataPoint(2, new float[]{0, 4});
		Assert.assertEquals(5.0f, DistanceMetric.EUCLIDEAN.calculate(p1, p2), 1.0e-8);
	}

	@Test
	public void testCosine() {
		// Try orthogonal vectors.
		DataPoint right = new DataPoint(0, new float[]{1.0f, 0.0f});
		DataPoint up = new DataPoint(1, new float[]{0.0f, 1.0f});
		DataPoint left = new DataPoint(2, new float[]{-1.0f, 0.0f});
		DataPoint down = new DataPoint(3, new float[]{0.0f, -1.0f});

		// Cosine DISTANCE not cosine SIMILARITY
		// 1 = completely different.
		// 0 = perfectly similar.
		Assert.assertEquals(0f, DistanceMetric.COSINE.calculate(right, right), 1.0e-8);
		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(right, up), 1.0e-8);
		Assert.assertEquals(1f, DistanceMetric.COSINE.calculate(right, left), 1.0e-8);
		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(right, down), 1.0e-8);

		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(up, right), 1.0e-8);
		Assert.assertEquals(0f, DistanceMetric.COSINE.calculate(up, up), 1.0e-8);
		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(up, left), 1.0e-8);
		Assert.assertEquals(1f, DistanceMetric.COSINE.calculate(up, down), 1.0e-8);

		Assert.assertEquals(1.0f, DistanceMetric.COSINE.calculate(left, right), 1.0e-8);
		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(left, up), 1.0e-8);
		Assert.assertEquals(0f, DistanceMetric.COSINE.calculate(left, left), 1.0e-8);
		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(left, down), 1.0e-8);

		Assert.assertEquals(0.5, DistanceMetric.COSINE.calculate(down, right), 1.0e-8);
		Assert.assertEquals(1.0f, DistanceMetric.COSINE.calculate(down, up), 1.0e-8);
		Assert.assertEquals(0.5f, DistanceMetric.COSINE.calculate(down, left), 1.0e-8);
		Assert.assertEquals(0f, DistanceMetric.COSINE.calculate(down, down), 1.0e-8);

		/*
		for(float i=0.0f; i < Math.PI*2; i+=0.1f) {
			for(float j=0.0f; j < Math.PI*2; j+=0.1f) {
				// i and j are our angles relative to 0-theta.
				// Calculate x and y, multiply them by random constants.
				DataPoint p1 = new DataPoint(1, new float[]{(float)Math.cos(i), (float)Math.sin(i)});
				DataPoint p2 = new DataPoint(2, new float[]{(float)Math.cos(j), (float)Math.sin(j)});
				Assert.assertEquals(Math.abs(i-j)*(Math.PI/2.0f), DistanceMetric.COSINE.calculate(p1, p2), 1.0e-2);
			}
		}
		*/
	}
}
