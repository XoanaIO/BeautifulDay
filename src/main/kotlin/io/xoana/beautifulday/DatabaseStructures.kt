package io.xoana.beautifulday;

import java.io.*
import java.net.InetSocketAddress
import java.util.*

/**
 * Created by jcatrambone on 6/16/17.
 */

data class Query(val qid:Int, val maxResults:Int=Int.MAX_VALUE, val point:DataPoint? = null, val distanceMetric: DistanceMetric? = null) : Serializable

class ResultSet(val query:Query) : Serializable {
	var resultCount = 0
	private val distances = FloatArray(query.maxResults, { _ -> Float.MAX_VALUE })
	private val ids = IntArray(query.maxResults, { _ -> -1 })

	fun getResultIDs(): IntArray {
		return ids.copyOf()
	}

	fun getResultDistances(): FloatArray {
		return distances.copyOf()
	}

	fun getResultIDDistancePairs(): Array<Pair<Int,Float>> {
		return Array<Pair<Int, Float>>(query.maxResults, { i ->
			Pair(ids[i], distances[i])
		})
	}

	fun joinSet(other: ResultSet): ResultSet {
		// Creates a new result set with _this_ maxResultSetSize entries.
		// TODO: Warn or assert if queries don't match?
		assert(this.query.qid == other.query.qid)

		// TODO: Optimization: If one set's max value is less than the other's min, just use all of it.
		// Create a new set.  Add elements from both.
		val resultSet = ResultSet(this.query)

		// Do something like the merge part of merge sort.
		var i:Int=0
		var j:Int=0
		while(resultSet.resultCount < this.query.maxResults && (i < this.resultCount || j < other.resultCount)) {
			if(j > other.distances.size || (this.distances[i] <= other.distances[j])) {
				// If we are out of items in the other set OR this distance is less than the other...
				resultSet.addResultToSet(this.ids[i], this.distances[i])
				i++
			} else if(i > this.distances.size || this.distances[i] >= other.distances[j]) {
				resultSet.addResultToSet(other.ids[j], other.distances[j])
				j++
			} else {
				// Both i and j are outside the size of the batches.
				break
			}
		}

		return resultSet
	}

	fun addResultToSet(id:Int, distance:Float) {
		// Insert the given item into the set, keeping the results in sorted order.
		var insertionIndex = Arrays.binarySearch(distances, distance)
		if(insertionIndex < 0) {
			insertionIndex = -(insertionIndex + 1)
		}
		// insertionIndex = (-(insertion point) - 1)
		// a = {0, 2, 4, 6, 8}
		// binsearch(0) = 0, binsearch(-1) = -1, binsearch(1) = -2, binsearch(10) = -6
		if(insertionIndex > query.maxResults-1) {
			// Do nothing.
			return
		}
		// Move all the elements over by one item and insert.
		for(i in query.maxResults-2 downTo insertionIndex) {
			ids[i+1] = ids[i]
			distances[i+1] = distances[i]
		}
		ids[insertionIndex] = id
		distances[insertionIndex] = distance
		resultCount++
	}

	fun toByteArray(): ByteArray {
		// TODO: Is it more efficient to include the min and max as part of serialization or calculate on the fly?
		val stream = ByteArrayOutputStream()
		val dout = DataOutputStream(stream)

		dout.writeInt(query.qid)
		dout.writeInt(query.maxResults)
		dout.writeInt(this.resultCount)
		for(i in 0 until this.resultCount) {
			dout.writeInt(ids[i])
			dout.writeFloat(distances[i])
		}
		return stream.toByteArray()
	}

	companion object {
		@JvmStatic
		fun fromByteArray(data: ByteArray): ResultSet {
			val stream = ByteArrayInputStream(data)
			val din = DataInputStream(stream)

			val qid = din.readInt()
			val maxResults = din.readInt()
			val numResults = din.readInt()
			val res = ResultSet(Query(qid, maxResults))
			for(i in 0 until numResults) {
				res.ids[i] = din.readInt()
				res.distances[i] = din.readFloat()
			}
			return res
		}
	}
}

data class DataPoint(val id:Int, val data:FloatArray) : Serializable {
	fun toByteArray(): ByteArray {
		val stream = ByteArrayOutputStream()
		val dout = DataOutputStream(stream)
		dout.writeInt(id)
		dout.writeInt(data.size)
		data.forEach { f -> dout.writeFloat(f) }
		return stream.toByteArray()
	}

	companion object {
		@JvmStatic
		fun fromByteArray(data: ByteArray): DataPoint {
			val stream = ByteArrayInputStream(data)
			val din = DataInputStream(stream)
			val pid = din.readInt()
			val numPoints = din.readInt()
			val pdata = FloatArray(numPoints, { _ -> din.readFloat() })
			return DataPoint(pid, pdata)
		}
	}
}

enum class DistanceMetric : Serializable {
	EUCLIDEAN {
		override fun calculate(p1:DataPoint, p2:DataPoint):Float {
			var accumulator = 0f
			p1.data.zip(p2.data).forEach { (a,b) -> accumulator += (a-b)*(a-b) }
			return Math.sqrt(accumulator.toDouble()).toFloat()
		}

		override fun calculateWithCutoff(p1:DataPoint, p2:DataPoint, cutoff:Float):Float? {
			val squaredCutoff = cutoff*cutoff // Square because we take SQRT as the last step.
			var accumulator = 0f
			for(i in 0 until p1.data.size) {
				val d = p1.data[i]-p2.data[i]
				accumulator += d*d
				if(accumulator > squaredCutoff) {
					return null
				}
			}
			return Math.sqrt(accumulator.toDouble()).toFloat()
		}
	},

	MANHATTAN {
		override fun calculate(p1:DataPoint, p2:DataPoint):Float {
			return p1.data.zip(p2.data).fold(0.0f, { accumulator, (a,b) -> accumulator + Math.abs(a-b) })
		}

		override fun calculateWithCutoff(p1:DataPoint, p2:DataPoint, cutoff:Float):Float? {
			return null
		}
	};

	abstract fun calculate(p1:DataPoint, p2:DataPoint):Float
	abstract fun calculateWithCutoff(p1:DataPoint, p2:DataPoint, cutoff:Float):Float? // Calculate distance, but stop if it exceeds cutoff.
}
