package Flink_Project.Learning.Components;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JedisConnectionTools {
	private static final Logger LOGGER = LoggerFactory.getLogger(JedisConnectionTools.class);

	/**
	 * Returns a list of connections generated from configuration string.
	 * 
	 * @param connectionStr Redis connection string as defined in the configuration
	 * @return List of connections
	 */
	public static List<JedisConnection> getJedisConnections(String connectionStr) {
		List<JedisConnection> result = new ArrayList<>();

		if (connectionStr == null) {
			return result;
		}

		String connections[] = connectionStr.split(",");

		for (String connection : connections) {
			String connectionParts[] = connection.split(":");

			if (connectionParts.length != 2) {
				LOGGER.error(String.format("Incorrect server:port string: %s", connection));
				continue;
			}

			try {
				String server = connectionParts[0];
				String portStr = connectionParts[1];

				int port = Integer.parseInt(portStr);
				JedisConnection con = new JedisConnection(server, port);
				LOGGER.debug("Created connection: " + con);
				result.add(con);
			} catch (NumberFormatException e) {
				LOGGER.error(String.format("Incorrect port: %s", connectionParts[1]));
				continue;
			}

		}

		return result;
	}

	/**
	 * A helper class to transfer server name and port.
	 *
	 */
	public static class JedisConnection {
		private String server;
		private int port;

		public JedisConnection(String server, int port) {
			super();
			this.server = server;
			this.port = port;
		}

		public String getServer() {
			return server;
		}

		public void setServer(String server) {
			this.server = server;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		@Override
		public String toString() {
			return "JedisConnection [server=" + server + ", port=" + port + "]";
		}

	}
}
