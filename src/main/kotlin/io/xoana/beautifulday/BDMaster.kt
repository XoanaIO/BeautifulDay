package io.xoana.beautifulday

import java.io.EOFException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.ServerSocket
import java.net.Socket

/**
 * Created by jcatrambone on 6/16/17.
 */
class BDMaster(listeningPort:Int) {
	var lastQueryID = 0
	val listenerThreads = mutableListOf<Thread>()
	private var quit = false
	//val sock:ServerSocketChannel = ServerSocketChannel.open()
	val sock:ServerSocket = ServerSocket(listeningPort)
	val workers = mutableListOf<Socket>()
	val workerReportsLeftToQueryCompletion = mutableMapOf<Int, MutableSet<Socket>>() // Query ID -> Set of Worker Indices
	val workerLastReportTime = mutableMapOf<Socket, Long>() // Worker index to last report.
	val activeQueries = mutableMapOf<Int, ResultSet>()
	val pendingWorkerMessages = mutableMapOf<Socket, MutableList<NetworkMessage>>()

	val numWorkers: Int
		get() = workers.size

	fun submitQuery(dataPoint: DataPoint, metric:DistanceMetric, numResults:Int): Int {
		lastQueryID++
		// A query goes to _all_ workers.
		assert(workers.size > 0)
		synchronized(workerReportsLeftToQueryCompletion, {
			workerReportsLeftToQueryCompletion[lastQueryID] = mutableSetOf<Socket>()
		})
		synchronized(pendingWorkerMessages, {
			workers.forEach {
				pendingWorkerMessages[it]!!.add(FindKNearestMessage(lastQueryID, numResults, metric, dataPoint))
				workerReportsLeftToQueryCompletion[lastQueryID]!!.add(it)
			}
		})
		return lastQueryID
	}

	fun submitQuery(floatArray: FloatArray, metric:DistanceMetric, numResults:Int): Int {
		val pt = DataPoint(-1, floatArray)
		return submitQuery(pt, metric, numResults)
	}

	fun getQueryResults(qid:Int): Array<Result>? {
		if(activeQueries[qid] == null) {
			return null // Not yet processed.  Waiting for worker to accept job.
		}
		// If the query is valid AND we have nobody left to report...
		if(workerReportsLeftToQueryCompletion[qid]?.isEmpty() ?: false) {
			synchronized(activeQueries, {
				val response = activeQueries.remove(qid)!!
				return response.getResults()
				// TODO: Make sure this releases the lock.
			})
		}

		return null
	}

	fun addPoint(dataPoint: DataPoint) {
		// A point is added only to one of the workers.
		pendingWorkerMessages[workers[dataPoint.id % workers.size]]!!.add(AddMessage(dataPoint))
	}

	fun shutdown() {
		// Send a shutdown to all workers.
		synchronized(pendingWorkerMessages) {
			workers.forEach { pendingWorkerMessages[it]!!.add(ShutdownMessage()) }
		}
		// Set terminate status.
		this.quit = true;
		val regThread = listenerThreads.removeAt(0) // This is our registration listener.  Special handling.
		regThread.interrupt() // Just kill it.
		listenerThreads.forEach { it.join() }
	}

	fun main() {
		//sock.configureBlocking(false)
		//sock.bind(InetSocketAddress(listeningPort))

		val registrationListener = Thread({
			// Registration thread...
			while(!quit && sock.isBound) {
				val socket = sock.accept()
				// INNER THREAD CREATED BY REG THREAD TO HANDLE REPORTS!
				// Create new thread to handle this socket registration.
				synchronized(listenerThreads, {
					val t = spinUpHandler(socket)
					t.name = "BDMaster_WorkListener_${listenerThreads.size}"
					listenerThreads.add(t)
					t.start()
				})
			}
		})
		registrationListener.name = "BDMaster_RegListener"
		registrationListener.start()
		listenerThreads.add(registrationListener)
	}

	fun spinUpHandler(socket:Socket): Thread {
		return Thread({
			val inStream = socket.getInputStream()
			val oin = ObjectInputStream(inStream)
			val outStream = socket.getOutputStream()
			val oout = ObjectOutputStream(outStream)

			// Is this thread registered?  Probably not if we're spinning up a thread.
			if(true) { // Force scoping.
				val msg = oin.readObject() as RegisterMessage
				assert(msg.type == NetworkMessageType.REGISTER)
				oout.writeChar('k'.toInt())
				synchronized(workers, { workers.add(socket) })
				synchronized(workerLastReportTime, { workerLastReportTime[socket] = System.currentTimeMillis() })
				synchronized(pendingWorkerMessages, { pendingWorkerMessages[socket] = mutableListOf<NetworkMessage>() })
				oout.flush()
			}

			while (socket.isConnected && !socket.isClosed && !quit) {
				try {
					var outboundMessages = mutableListOf<NetworkMessage>()
					synchronized(pendingWorkerMessages, { outboundMessages = pendingWorkerMessages[socket]!! })
					while(outboundMessages.isNotEmpty()) {
						val outboundMsg:NetworkMessage? = outboundMessages.removeAt(0) // Remove first.
						if(outboundMsg == null) {
							println("BDMaster: Somehow a null outbound message was placed on the queue.")
							continue
						}
						oout.writeObject(outboundMsg) // Write the message.
						oout.flush()
						// Then deal with the fallout and followup.
						when(outboundMsg.type) {
							NetworkMessageType.ADD -> null // Nothing else needed.
							NetworkMessageType.REGISTER -> null // Already handled.  Maybe should fault, since Master doesn't reg.
							NetworkMessageType.SHUTDOWN -> socket.close() // We should close this socket.
							NetworkMessageType.FIND_K_NEAREST -> {
								// Message is written.  Wait for reply.
								val res = oin.readObject() as ResultSet
								val fkMsg = outboundMsg as FindKNearestMessage
								synchronized(workerReportsLeftToQueryCompletion, {
									workerReportsLeftToQueryCompletion[fkMsg.id]!!.remove(socket)
								})
								synchronized(activeQueries, {
									activeQueries[fkMsg.id] = activeQueries[fkMsg.id]?.joinSet(res) ?: res
								})
								//activeQueries[(msg as FindKNearestMessage).id]!!.joinSet()
							}
							NetworkMessageType.REMOVE -> TODO()
							NetworkMessageType.INVALID -> TODO()
						}

						workerLastReportTime[socket] = System.currentTimeMillis()
					}
					Thread.yield() // Enhance your calm.
				} catch(eofe:EOFException) {
					// We're just shutting down.  It's okay.
				}
			}
			if(!socket.isClosed) {
				socket.close()
			}
		})
	}
}