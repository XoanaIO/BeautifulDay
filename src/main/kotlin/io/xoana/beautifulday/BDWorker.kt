package io.xoana.beautifulday

import java.io.*
import java.net.InetSocketAddress
import java.net.Socket

/**
 * Created by jcatrambone on 6/16/17.
 */

class BDWorker(val master: InetSocketAddress) {
	var storageFile: String = ""

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
			var rawMsg: Any? = null
			try {
				rawMsg = oin.readObject()
			} catch(eofe:EOFException) {
				println("BDWorker: EOF while waiting for message.  Assuming shutdown.")
				rawMsg = ShutdownMessage()
			}
			if(rawMsg == null) {
				println("BDWorker: Got a null object read.  What the hell?")
				continue
			}

			// Translate message
			val msg:NetworkMessage = rawMsg as NetworkMessage
			when (msg.type) {
				NetworkMessageType.REGISTER -> Unit // Do we want to handle this?
				NetworkMessageType.ADD -> addPoint((msg as AddMessage).point)
				NetworkMessageType.FIND_K_NEAREST -> performSearch(messageToQuery(msg))
				NetworkMessageType.REMOVE -> removePoint((msg as RemoveByIDMessage).id)
				NetworkMessageType.SHUTDOWN -> {
					// Save our local copy to disk.
					saveToDisk()
					quit = true
					sock.close()
				}
				else -> {
					Exception("Unhandled network message.  Type: ${msg.type}")
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

	fun removePoint(id:Int) {
		if(id in index) {
			data.removeAt(index[id]!!)
			index.remove(id)
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

	// NOTE: We allow the user to override filename because sometimes you'll want to read from a different file at startup.
	fun loadFromDisk(filename:String = storageFile) {
		val fin = File(filename).bufferedReader()
		fin.lines().forEach { line ->
			val tokens = line.split('\t')
			val id = tokens[0].toInt()
			val data = tokens[1]
			val dp = DataPoint(id, data.split(',').map { it.toFloat() }.toFloatArray())
			addPoint(dp)
		}
		fin.close()
	}

	fun saveToDisk(filename:String = storageFile) {
		val fout = if(storageFile == "") {
			File.createTempFile("beautilfulday_worker", ".dat").bufferedWriter()
		} else {
			File(filename).bufferedWriter()
		}
		index.forEach { pointIDIndexPair ->
			fout.write(pointIDIndexPair.key)
			fout.write("\t")
			fout.write(data[pointIDIndexPair.value].data.joinToString(separator = ","))
			fout.write("\n")
		}
		fout.close()
	}
}
