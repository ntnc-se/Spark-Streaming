
package com.esotericsoftware.tcpserver;

import static com.esotericsoftware.tcpserver.Util.*;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class BroadcastServer extends Retry {
	static public final byte[] prefix = new byte[] {62, 126, -17, 61, 127, -16, 63, 125, -18};

	private int port;
	private DatagramSocket socket;
	private final byte[] buffer = new byte[prefix.length];

	public BroadcastServer (String category, String name) {
		this(category, name, 0);
	}

	public BroadcastServer (String category, String name, int port) {
		super(category, name);
		this.port = port;
	}

	protected void retry () {
		try {
			socket = new DatagramSocket(port);
		} catch (Exception ex) {
			failed();
			return;
		}
		success();
		try {
			byte[] receiveBuffer = receiveBuffer();
			outer:
			while (running) {
				DatagramPacket packet = new DatagramPacket(receiveBuffer, receiveBuffer.length);
				try {
					socket.receive(packet);
				} catch (SocketException ex) {
					if (!running) return;
					throw ex;
				}

				int n = prefix.length;
				if (packet.getLength() < n) {
				} else {
					for (int i = 0; i < n; i++) {
						if (receiveBuffer[i] != prefix[i]) {

							continue outer;
						}
					}

					byte[] responseBuffer = responseBuffer(packet);
					System.arraycopy(prefix, 0, responseBuffer, 0, prefix.length);
					packet = new DatagramPacket(responseBuffer, responseBuffer.length, packet.getAddress(), packet.getPort());
					socket.send(packet);
				}
			}
		} catch (Exception ex) {
			closeQuietly(socket);
			failed();
		} finally {
		}
	}

	/** Returns the buffer into which the received packet will be written. It must be at least large enough for {@link #prefix}. */
	protected byte[] receiveBuffer () {
		return buffer;
	}

	/** Returns the buffer which is sent as a response. It must be at least large enough for {@link #prefix}, which is written at
	 * the start. */
	protected byte[] responseBuffer (DatagramPacket packet) {
		return buffer;
	}

	protected void stopped () {
		closeQuietly(socket);
	}

	public int getPort () {
		return port;
	}

	public void setPort (int port) {
		this.port = port;
	}

	static public void main (String[] args) throws Exception {

		BroadcastServer server = new BroadcastServer("broadcast", "test", 53333);
		server.start();

		System.out.println(BroadcastClient.find(53333, 1000).getAddress());
	}
}
