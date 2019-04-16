package Flink_Project.Learning;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import Flink_Project.Learning.Components.DeduplicationHashCalculatorMapFunction;
import Flink_Project.Learning.Components.RedisDeduplicationFilterFunction;

public class FlinkDataStreamProcessingJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> dataStream = executionEnvironment.fromElements("This is a first sentence",
				"This is a first sentence", "This is a second sentence");

		DataStream<String> upperCase = dataStream.map(String::toUpperCase).name("Convert String into UpperCase")
				.rebalance();

		SingleOutputStreamOperator<String> filterString = upperCase.filter(new RedisDeduplicationFilterFunction())
				.map(new DeduplicationHashCalculatorMapFunction());

		filterString.print();
		executionEnvironment.execute();
	}

}
