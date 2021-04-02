package SecondarySort;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;


public class SecondarySortBasicMapper extends Mapper<Void, GenericRecord, CompositeKeyWritable, Text> {

	// Config parquet file projection (projetar só as colunas que queremos)
	public static Schema getSchema() throws IOException {
		InputStream is = new FileInputStream("schema_secondary_sort.parquet");
		String ps = new String(is.readAllBytes());
		MessageType mt = MessageTypeParser.parseMessageType(ps);
		return new AvroSchemaConverter().convert(mt);
	}

	@Override
	public void map(Void key, GenericRecord value, Context context)
			throws IOException, InterruptedException {

		// guardar os dados do titulo numa lista: tipo, nome primario, nome original
		List<GenericRecord> title = (List<GenericRecord>) value.get("titles");

		// guardar a tipologia
		String tipologia =title.get(0).get(0).toString();


		//verificar se é um registo do tipo "movie"
		if (tipologia.equals("movie"))  {

			//guardar lista de generos
			List<String> genres = (List<String>) value.get("genres");
			//selecionar o primeiro genero da lista
			String genre = genres.get(0);

			//guardar o valor do rating
			Float rating =  (Float) value.get(7);

			//guardar o nome do filme
			String filme = title.get(0).get(1).toString();

			//criar uma chaves composta com o genero e o rating
			CompositeKeyWritable comp_key= new CompositeKeyWritable( genre, rating);

			//enviar a chave composta e o titulo do filme para o reduce
			context.write(comp_key, new Text(filme));
		}

	}
}