package Flink_Project.Learning.Components;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;

import Flink_Project.Learning.Components.JedisConnectionTools.JedisConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * A map function for calculating a unique hash string based on file's content.
 * The hash will be used at the next step to find duplicate files.<br>
 * 
 *
 */
public class DeduplicationHashCalculatorMapFunction extends RichMapFunction<String, String> {

	private static final long serialVersionUID = 1934638002628361447L;

	private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicationHashCalculatorMapFunction.class);
	
	private static boolean redisDeduplicationEnabled = true;
	Jedis jedis = null;
	
	@Override
	public String map(String value) throws Exception {

		String hash = Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();

		LOGGER.info("For unique string: " + value + " ; hash: " + hash);

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
		
		jedis.set(value, hash, "NX", "EX", 604800);
		
		return hash;
	}

}
