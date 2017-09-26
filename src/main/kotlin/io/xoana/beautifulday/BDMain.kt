package io.xoana.beautifulday

import io.javalin.Context
import io.javalin.Javalin
import java.io.FileNotFoundException
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.util.concurrent.atomic.AtomicBoolean


/**
 * Created by jcatrambone on 6/22/17.
 */

const val DEFAULT_WORKER_PORT = 9876
const val DEFAULT_WEB_PORT = 8888
const val USAGE = """
Beautiful Day Nearest Neighbor Search
===
java -jar bd.jar worker -h host [-p port] [-d data file name]
	-h <host> the master node to which the worker should connect.
	-p <port> the port number to which this worker should connect.  Default: $DEFAULT_WORKER_PORT.
	-d refers to the data file which should be loaded on startup.
java -jar bd.jar master [-p port] [--webport port]
	-p <port> the port on which the master will listen for worker conneections.  Default: $DEFAULT_WORKER_PORT
	--webport <port> the port on which the master will listen for rest connections.  Default: $DEFAULT_WEB_PORT
"""

fun main(args: Array<String>) {
	// Depending on the args, this could be the CLI, the master runner, or the client runner.
	// java -jar bd.jar worker
	// java -jar bd.jar master
	// java -jar bd.jar cli
	if(args.isEmpty()) {
		println(USAGE)
		return
	}

	if(args[0].equals("worker", true)) {
		val port = findArgumentAfterString("-p", args, "$DEFAULT_WORKER_PORT").toInt()
		val host = findArgumentAfterString("-h", args, "")
		val datafile = findArgumentAfterString("-d", args, "")
		println("Starting worker.  Connecting to master at $host:$port");
		val w = BDWorker(InetSocketAddress(host, port))
		w.storageFile = datafile
		try {
			w.loadFromDisk()
		} catch(fnfe: FileNotFoundException) {} // Expected if it's the first start.
		w.main()
	} else if(args[0].equals("master", true)) {
		// Open our master...
		val workerListenPort = findArgumentAfterString("-p", args, "$DEFAULT_WORKER_PORT").toInt()
		val restListenPort = findArgumentAfterString("--webport", args, "$DEFAULT_WEB_PORT").toInt()
		val m = BDMaster(workerListenPort)
		m.main()

		// Build and attach a CLI.
		/*
		val defaultTerminalFactory = DefaultTerminalFactory()
		val terminal = defaultTerminalFactory.createTerminal()
		terminal.enterPrivateMode() // Should give us a 'fullscreen' window.
		terminal.clearScreen() // Should happen by default on the above, unless we've got a crap terminal.
		terminal.setCursorVisible(false)
		val textGraphics = terminal.newTextGraphics()
		*/

		// Set paths.
		val quit = AtomicBoolean(false)
		val app:Javalin = Javalin.create().port(restListenPort);
		app.get("/ping", {ctx -> ctx.result("Pong")});
		app.post("/add", {ctx ->
			// If the URL doesn't provide point, must be in body.
			val id = (ctx.param("id") ?: ctx.bodyParam("id"))!!.toInt()
			val pointString = ctx.param("point") ?: URLDecoder.decode(ctx.bodyParam("point")!!, "UTF-8")
			val data = pointString.split(',').map { it.toFloat() }.toFloatArray()
			val point = DataPoint(id, data)

			m.addPoint(point)

			ctx.status(201) // Created.
			ctx.result("ok")
			println("Added point $id")
		})
		// Finding via the get method (which accepts params as args.)
		app.get("/find/:k/:point", {ctx ->
			val k = ctx.param("k")!!.toInt()
			val pointString = ctx.param("point")!!
			val data = pointString.split(',').map { it.toFloat() }.toFloatArray()

			find(m, ctx, k, data, DistanceMetric.EUCLIDEAN)
		})
		app.post("/find", {ctx ->
			val k = ctx.bodyParam("k")!!.toInt()
			val pointString = URLDecoder.decode(ctx.bodyParam("point")!!, "UTF-8")
			val data = pointString.split(",").map {it.toFloat() }.toFloatArray()

			find(m, ctx, k, data, DistanceMetric.EUCLIDEAN)
		})
		// Maybe we shouldn't expose this.
		app.post("/shutdown", {ctx ->
			quit.set(true)
			ctx.status(200)
			ctx.result("ok")
		})

		//terminal.addResizeListener()
		//readInput is blocking.  pollInput is async and returns null if there's nothing.
		println("Server is up.  It's a beautiful day in my neighborhood.")
		println("Listening for workers on $workerListenPort")
		println("Listening for REST requests on $restListenPort")
		while(!quit.get()) {
			try {
				Thread.sleep(1000)
			} catch (ie:InterruptedException) {
				quit.set(true)
			}
			/*
			textGraphics.foregroundColor = TextColor.ANSI.YELLOW
			textGraphics.putString(0, 0, "BeautifulDay Master : Listening on $port")
			textGraphics.foregroundColor = TextColor.ANSI.DEFAULT
			textGraphics.putString(0, 1, "Workers connected: ${m.workers.size}")
			m.workers.forEachIndexed{ i, w ->
				textGraphics.putString(0, 2+i, "${w.remoteSocketAddress}")
			}
			textGraphics.putString(0, terminal.terminalSize.rows-1, "Press Escape to Quit")
			val keyStroke: KeyStroke? = terminal.pollInput()
			quit = (keyStroke != null && (keyStroke.keyType == KeyType.Escape))
			terminal.flush()
			*/
		}

		// Tear down the terminal.
		/*
		terminal.exitPrivateMode()
		terminal.resetColorAndSGR()
		terminal.close()
		*/
		println("Shutting down...")
		m.shutdown()
		println("Have a nice day.")
		System.exit(0)
	} else {
		println("Unrecognized running mode: $args[0]")
		println(USAGE)
	}
}

private fun find(m:BDMaster, ctx: Context, k:Int, data:FloatArray, metric:DistanceMetric) {
	val qid = m.submitQuery(data, metric, k)
	println("QUERY $qid launched query for top $k points.")
	var tempResultSet:Array<Result>? = null
	while(tempResultSet == null) {
		tempResultSet = m.getQueryResults(qid)
		Thread.yield()
	}
	println("QUERY $qid returned")

	val resultSet = tempResultSet
	ctx.json(resultSet)
}

// This could use some explanation.
// Start with a default value for the return.  If the item we encounter matches the string we're seeking, set accum (which will be returned) to the value of the next index.
private fun findArgumentAfterString(str:String, args:Array<String>, default:String=""):String = args.foldRightIndexed(default, {ind, s, acc -> if(s.equals(str)) { args[ind+1] } else { acc }})
