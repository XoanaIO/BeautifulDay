package io.xoana.beautifulday

import com.googlecode.lanterna.TextColor
import com.googlecode.lanterna.input.KeyStroke
import com.googlecode.lanterna.input.KeyType
import java.net.InetSocketAddress
import com.googlecode.lanterna.terminal.DefaultTerminalFactory



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
	if(args.size == 0) {
		println(USAGE)
		return
	}

	if(args[0].equals("worker", true)) {
		val port = findArgumentAfterString("-p", args, "0").toInt()
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
		var quit = false
		val defaultTerminalFactory = DefaultTerminalFactory()
		val terminal = defaultTerminalFactory.createTerminal()
		terminal.enterPrivateMode() // Should give us a 'fullscreen' window.
		terminal.clearScreen() // Should happen by default on the above, unless we've got a crap terminal.
		terminal.setCursorVisible(false)
		val textGraphics = terminal.newTextGraphics()
		//terminal.addResizeListener()
		//readInput is blocking.  pollInput is async and returns null if there's nothing.
		while(!quit) {
			textGraphics.foregroundColor = TextColor.ANSI.YELLOW
			textGraphics.putString(0, 0, "BeautifulDay Master : Listening on $port")
			textGraphics.foregroundColor = TextColor.ANSI.DEFAULT
			textGraphics.putString(0, 1, "Workers connected: ${m.workers.size}")
			m.workers.forEachIndexed{ i, w ->
				textGraphics.putString(0, 2+i, "${w.remoteSocketAddress}")
			}
			textGraphics.putString(terminal.terminalSize.columns-1, 0, "Press Escape to Quit")
			val keyStroke: KeyStroke? = terminal.pollInput()
			quit = (keyStroke != null && (keyStroke.keyType == KeyType.Escape))
			terminal.flush()
		}
		terminal.exitPrivateMode()
		terminal.resetColorAndSGR()
		terminal.close()
		println("Shutting down...")
		m.shutdown()
		println("Have a nice day.")
	} else {
		println("Unrecognized running mode: $args[0]")
		println(USAGE)
	}
}

// This could use some explanation.
// Start with a default value for the return.  If the item we encounter matches the string we're seeking, set accum (which will be returned) to the value of the next index.
private fun findArgumentAfterString(str:String, args:Array<String>, default:String=""):String = args.foldRightIndexed(default, {ind, s, acc -> if(s.equals(str)) { args[ind+1] } else { acc }})

