package Flink_Project.Learning;

import static org.assertj.core.api.Assertions.assertThat; // main one

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Hello world!
 *
 */
public class FlinkProcessingJob 
{
    public static void main( String[] args ) throws Exception
    {
    	ExecutionEnvironment env
    	  = ExecutionEnvironment.getExecutionEnvironment();
    	
    	DataSet<Integer> amounts = env.fromElements(1, 29, 40, 50);
    	
    	final int threshold = 30;
    	List<Integer> collect = amounts
    	  .filter(a -> a > threshold)
    	  .reduce((integer, t1) -> integer + t1)
    	  .collect();
    	
    	assertThat(collect.get(0)).isEqualTo(90);
    }
}
