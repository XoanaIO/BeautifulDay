package io.xoana.beautifulday

import java.io.*
import java.net.Socket

/**
 * Created by jcatrambone on 6/18/17.
 */

enum class NetworkMessageType : Serializable {
	REGISTER, SHUTDOWN, ADD, FIND_K_NEAREST, REMOVE, INVALID
}

open class NetworkMessage(val type:NetworkMessageType) : Serializable

class RegisterMessage() : NetworkMessage(NetworkMessageType.REGISTER)

class AddMessage(val point:DataPoint) : NetworkMessage(NetworkMessageType.ADD)

class FindKNearestMessage(val id: Int, val k: Int, val metric: DistanceMetric, val point: DataPoint) : NetworkMessage(NetworkMessageType.FIND_K_NEAREST)

class ShutdownMessage : NetworkMessage(NetworkMessageType.SHUTDOWN)