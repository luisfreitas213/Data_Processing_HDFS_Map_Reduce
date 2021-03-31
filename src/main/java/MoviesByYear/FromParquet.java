package MoviesByYear;

import com.fasterxml.jackson.databind.util.TypeKey;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;


public class FromParquet {
    // Config parquet file
    public static Schema getSchemapro() throws IOException {
        InputStream is = new FileInputStream("schema2.parquet");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }
    public static Schema getSchema() throws IOException {
        InputStream is = new FileInputStream("schemaoutput.parquet");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static class FromParquetMapper extends Mapper<Void, GenericRecord, Text, Text>{


        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            //falta filtrar por movies
            List<GenericRecord> title = (List<GenericRecord>) value.get("titles");
            String tipologia =title.get(0).get(0).toString();

            if(value.get(3)!=null && tipologia.equals("movie"))  {
                String s = (String) value.get(0);
                s= s+"\t"+title.get(0).get(0).toString();
                s= s+"\t"+title.get(0).get(1).toString();
                s= s+"\t"+title.get(0).get(2).toString();
                s = s+"\t"+ value.get(1);
                s = s+"\t"+ value.get(7);
                s = s+"\t"+ value.get(8);

                String ano =value.get(3).toString();

                context.write(new Text(ano), new Text(s));
            }
        }

    }

    private static Map<Integer, Float> sortByValue(Map<Integer, Float> unsortMap) {

        // 1. Convert Map to List of Map
        List<Map.Entry<Integer, Float>> list =
                new LinkedList<Map.Entry<Integer, Float>>(unsortMap.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>() {
            public int compare(Map.Entry<Integer, Float> o1,
                               Map.Entry<Integer, Float> o2) {
                //return (o1.getValue()).compareTo(o2.getValue()); change order to sort ascending
                return (o2.getValue()).compareTo(o1.getValue()); // sort descending
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<Integer, Float> sortedMap = new LinkedHashMap<Integer, Float>();
        for (Map.Entry<Integer, Float> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }


        return sortedMap;
    }

    public static class FromParquetReducer extends Reducer<Text,Text,Void, GenericRecord> {
        //Declare Variables to Schema output
        private Schema schema;
        private Schema tschema;

        //Executar a configuração do schema output


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //Principal Schema
            schema = getSchema();
            tschema = schema.getField("filmRank").schema().getElementType();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int total=0;
            int max_votos=0;
            String movie_max_votos = null;
            ArrayList sub_rank = new ArrayList<>();
            ArrayList rank = new ArrayList<>();
            Map<Integer, Float> RatingsMap = new HashMap<>();
            Map<Integer, String> MoviesMap = new HashMap<>();


            System.out.println("Ano " + key);

            for(Text value: values) {
                total += 1;
                String[] movie = value.toString().split("\t");
                int votes=Integer.parseInt(movie[6]);
                float rating=Float.parseFloat(movie[5]);
                String movie_name = movie[3];
                if(votes>=max_votos)
                {
                    movie_max_votos=movie_name;
                }
                RatingsMap.put(total,rating);
                MoviesMap.put(total, movie_name);
                sub_rank.add(movie_name);
            }

            RatingsMap=sortByValue(RatingsMap);
            System.out.println(RatingsMap);
            List<Integer> keys = RatingsMap.entrySet().stream()
                    .map(Map.Entry::getKey)
                    .limit(10)
                    .collect(Collectors.toList());

            System.out.println(keys);

            String content = "[";

            for (int indice:keys){
                rank.add(sub_rank.get(indice - 1));
            }
            for (int i = 0; i < rank.size(); i++){
                System.out.println(rank.get(i));
                int j = i+1;
                content = content + j +": "+ rank.get(i) +"; ";
            }

            content = total + " " + movie_max_votos +" "+ content+"]";

            //context.write(new Text(key.toString()),new Text(content));

            //Insert output
            GenericRecord record = new GenericData.Record(schema);
            //insert year
            record.put("year", key.toString());
            //insert numFilms
            record.put("numFilms", total);
            //insert bestMovie
            record.put("bestMovie",movie_max_votos);
            //insert filmRank
            List<GenericRecord> filmranks = new ArrayList<>();
            GenericRecord trecord = new GenericData.Record(tschema);
            for (int i = 0; i < rank.size(); i++) {
                trecord.put("position", i + 1);
                trecord.put("name", rank.get(i));
                filmranks.add(trecord);
            }
            record.put("filmRank", filmranks);



            //output parquet
            context.write(null, record);
            //configurar o job e testar
        }
    }


    public static void main(String[] args) throws Exception{

        Job job = Job.getInstance(new Configuration(), "FromParquet");
        job.setJarByClass(MoviesByYear.FromParquet.class);
        job.setMapperClass(MoviesByYear.FromParquet.FromParquetMapper.class);
        job.setReducerClass(FromParquet.FromParquetReducer.class);

        //Configurar os tipos de dados que vão do mapper para o reduce
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Configurar a conversão dos dados do reduce para o ficheiro final
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path("output"));
        AvroParquetInputFormat.setRequestedProjection(job,getSchemapro());

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job,getSchema());
        FileOutputFormat.setOutputPath(job, new Path("MoviesByYear"));

        job.waitForCompletion(true);
    }
}
