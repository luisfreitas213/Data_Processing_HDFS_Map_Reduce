package MapReduce;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;



public class FromParquet {

    public static class FromParquetMapper extends Mapper<Void, GenericRecord, Void, Text>{

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            context.write(null,new Text(String.valueOf(value)));
        }
    }

    public static void main(String[] args) throws Exception{

        Job job = Job.getInstance(new Configuration(), "FromParquet");
        job.setJarByClass(FromParquet.class);
        job.setMapperClass(FromParquetMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path("output"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("text"));

        job.waitForCompletion(true);
    }
}
