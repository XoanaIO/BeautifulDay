package io.xoana.beautifulday

import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket

/**
 * Created by jcatrambone on 6/16/17.
 */

class BDWorker(val master: InetSocketAddress) {
	val index = mutableMapOf<Int, Int>()
	val data = mutableListOf<DataPoint>()

	// Open the memory-mapped path to our point store.
	/*
	val entrySize = 4*dimensions //32 bits per float in kotlin.  4 bytes.
	val file = File(memoryMapPath)
	val fileChannel: FileChannel = RandomAccessFile(file, "w+").channel
	val mmap: MappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size())
	*/

	// Open a socket and register with the work master.
	lateinit var inStream: InputStream
	lateinit var outStream: OutputStream
	lateinit var oin: ObjectInputStream
	lateinit var oout: ObjectOutputStream

	var quit = false

	fun main() {
		// TODO: If we have an index, load it.

		// Do all our registration and connecting.
		val sock: Socket = Socket(master.address, master.port)
		outStream = sock.getOutputStream()
		oout = ObjectOutputStream(outStream)
		inStream = sock.getInputStream()
		oin = ObjectInputStream(inStream)
		oout.writeObject(RegisterMessage())
		assert(oin.readChar() == 'k') // Ack.

		// Main loop.
		while(!quit && sock.isConnected && !sock.isClosed) {
			// Read a command from thread master.
			// Don't need the BufferedReader(InputStreamReader()) wrapper here 'cause Kotlin!

			val rawMsg:Any? = oin.readObject()
			if(rawMsg == null) {
				println("BDWorker: Got a null object read.  What the hell?")
				continue
			}
			val msg:NetworkMessage = rawMsg as NetworkMessage
			when (msg.type) {
				NetworkMessageType.REGISTER -> null // Do we want to handle this?
				NetworkMessageType.ADD -> addPoint((msg as AddMessage).point)
				NetworkMessageType.FIND_K_NEAREST -> performSearch(messageToQuery(msg))
				NetworkMessageType.REMOVE -> TODO()
				NetworkMessageType.SHUTDOWN -> {
					// Save our local copy to disk.
					saveToDisk()
					quit = true
					sock.close()
					null
				}
				else -> {
					Exception("Unhandled network message.  Type: ${msg.type}")
					null
				}
			}
		}
	}

	fun addPoint(point:DataPoint) {
		// Does this item already exist?  If so, replace it.
		if(point.id in index) {
			data.set(index[point.id]!!, point)
		} else {
			data.add(point)
			index[point.id] = data.size-1
		}
	}

	fun messageToQuery(msg:NetworkMessage):Query {
		val fmsg = msg as FindKNearestMessage
		return Query(fmsg.id, fmsg.k, fmsg.point, fmsg.metric)
	}

	fun performSearch(query:Query) {
		// Create our result set.
		val results = ResultSet(query)
		// Search the points for the top K.
		data.forEach { pt ->
			val distance = query.distanceMetric!!.calculate(query.point!!, pt)
			results.addResultToSet(pt.id, distance)
		}
		// Add to result store.
		//unfetchedResults[query.qid] = results
		oout.writeObject(results)
	}

	fun loadFromDisk(filename:String = "") {

	}

	fun saveToDisk(filename:String = "") {

	}
}
