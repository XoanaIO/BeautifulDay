package io.xoana.beautifulday

import io.javalin.Javalin
import java.net.InetSocketAddress


/**
 * Created by jcatrambone on 6/22/17.
 */

const val USAGE = """
Beautiful Day Nearest Neighbor Search
===
java -jar bd.jar worker -h host -p port (-d data file name)
	-h <host> and -p <port> refer to the master node to which they should connect.
	-d refers to the data file which should be loaded on startup.  If none is specified, will use /tmp/ dir.
java -jar bd.jar worker -p port
	-p is the listening port.
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
		val port = findArgumentAfterString("-p", args, "9876").toInt()
		val host = findArgumentAfterString("-h", args, "")
		val datafile = findArgumentAfterString("-d", args, "")
		if(datafile != "") {
			TODO("Datafile target is not yet implemented.")
		}
		println("Starting worker.  Connecting to master at $host:$port");
		val w = BDWorker(InetSocketAddress(host, port))
		w.main()
	} else if(args[0].equals("master", true)) {
		// Open our master...
		val port = findArgumentAfterString("-p", args, "9876").toInt()
		val m = BDMaster(port)
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
		val app:Javalin = Javalin.create().port(7000);
		app.get("/ping", {ctx -> ctx.result("Pong")});
		app.post("/add/:id/:point", {ctx ->
			val id = ctx.param("id")!!.toInt()
			val pointString = ctx.param("point")!!
			val data = pointString.split(',').map { it.toFloat() }.toFloatArray()
			val point = DataPoint(id, data)

			m.addPoint(point)

			ctx.status(201) // Created.
			ctx.result("ok")
			println("Added point $id")
		})
		app.get("/find/:k/:point", {ctx ->
			val k = ctx.param("k")!!.toInt()
			val pointString = ctx.param("point")!!
			val data = pointString.split(',').map { it.toFloat() }.toFloatArray()
			val qid = m.submitQuery(data, DistanceMetric.EUCLIDEAN, k)

			println("QUERY $qid launched query for top $k points.")
			var tempResultSet:Array<Result>? = null
			while(tempResultSet == null) {
				tempResultSet = m.getQueryResults(qid)
				Thread.yield()
			}
			println("QUERY $qid returned")

			val resultSet = tempResultSet
			ctx.json(resultSet)
		})

		//terminal.addResizeListener()
		//readInput is blocking.  pollInput is async and returns null if there's nothing.
		println("Server is up.  It's a beautiful day in my neighborhood.")
		var quit = false
		while(!quit) {
			Thread.yield()
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
		println("Shutting down...")
		m.shutdown()
		println("Have a nice day.")
		System.exit(0)
		*/
	} else {
		println("Unrecognized running mode: $args[0]")
		println(USAGE)
	}
}

// This could use some explanation.
// Start with a default value for the return.  If the item we encounter matches the string we're seeking, set accum (which will be returned) to the value of the next index.
private fun findArgumentAfterString(str:String, args:Array<String>, default:String=""):String = args.foldRightIndexed(default, {ind, s, acc -> if(s.equals(str)) { args[ind+1] } else { acc }})
