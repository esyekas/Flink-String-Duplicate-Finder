package Flink_Project.Learning.Components;

import java.util.List;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Flink_Project.Learning.Components.JedisConnectionTools.JedisConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * A filter function that check if the file hash is present in Redis. If present
 * - the file is filtered out.
 *
 */
public class RedisDeduplicationFilterFunction extends RichFilterFunction<String> {

	private static final long serialVersionUID = 3141812663046437596L;

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisDeduplicationFilterFunction.class);

	private static boolean redisDeduplicationEnabled = true;

	static Jedis jedis = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		if (redisDeduplicationEnabled) {
			try {
				String connectionStr = "localhost:6379";
				List<JedisConnection> connections = JedisConnectionTools.getJedisConnections(connectionStr);

				if (connections.size() > 0) {
					JedisConnection connection = connections.get(0);
					jedis = new Jedis(connection.getServer(), connection.getPort());
					LOGGER.info(String.format("Jedis initialized and connected to Redis server %s:%s Redis PING: %s",
							connection.getServer(), connection.getPort(), jedis.ping()));
				} else {
					LOGGER.error("No connection details for Redis found! Disabling Redis deduplication.");
					redisDeduplicationEnabled = false;
				}
			} catch (JedisException e) {
				LOGGER.error("JedisException when initializing Redis connection! Disabling Redis deduplication.", e);
				redisDeduplicationEnabled = false;
			}
		} else {
			LOGGER.info("Redis-based deduplication is disabled in configuration.");
		}
	}

	@Override
	public boolean filter(String value) {
		if (!redisDeduplicationEnabled) {
			return true;
		}

		String result = null;
		try {
			result = jedis.get(value);
		} catch (JedisException e) {
			LOGGER.error("JedisException when checking if file is a duplicate! "
					+ "The file will be considered as unique to prevent data loss and not stop data flow.", e);
			return true;
		}

		if (result == null) {
			return true;
		} else {
			LOGGER.warn("Duplicate file found!!! hash: " + value);
			return false;
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (redisDeduplicationEnabled) {
			try {
				//resetRedis();
				jedis.quit();
				jedis.disconnect();
			} catch (Throwable t) {
				LOGGER.error("Error while closing Redis connection at close(): " + t);
			}
		}
	}

	public static void resetRedis() {

		if (redisDeduplicationEnabled) {
			try {
				jedis.flushAll();
				LOGGER.info("Remove all keys from all databases");
			} catch (Throwable t) {
				LOGGER.error("Error while closing Redis connection at close(): " + t);
			}
		}
	}
}