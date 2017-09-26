package io.xoana.beautifulday

import java.io.*
import java.net.Socket

/**
 * Created by jcatrambone on 6/18/17.
 */

enum class NetworkMessageType : Serializable {
	REGISTER, SHUTDOWN, ADD, FIND_K_NEAREST, REMOVE, INVALID
}

open class NetworkMessage(val type:NetworkMessageType) : Serializable {
	// Disable while in development.  Enable to protect against version conflicts.
	/*
	companion object {
		private const val serialVersionUID: Long = 20170707133014
	}
	*/
}

class RegisterMessage() : NetworkMessage(NetworkMessageType.REGISTER)

class AddMessage(val point:DataPoint) : NetworkMessage(NetworkMessageType.ADD)

class FindKNearestMessage(val id: Int, val k: Int, val metric: DistanceMetric, val point: DataPoint) : NetworkMessage(NetworkMessageType.FIND_K_NEAREST)

class RemoveByIDMessage(val id: Int) : NetworkMessage(NetworkMessageType.REMOVE)

class ShutdownMessage : NetworkMessage(NetworkMessageType.SHUTDOWN)