package SecondarySort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


//O particionador decide para que tarefa de reduce irá cada operação de map com base na chave de saída
public class SecondarySortBasicPartitioner extends
  	Partitioner<CompositeKeyWritable, NullWritable> {


	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value,
			int numReduceTasks) {

		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}