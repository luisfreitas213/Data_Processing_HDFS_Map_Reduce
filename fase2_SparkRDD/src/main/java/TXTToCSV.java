import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TXTToCSV {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("g4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final String fileDir = "file:////home/mcmaia/GGCD/dados/mini/";

        JavaRDD<String> actors = sc.textFile(fileDir + "name.basics.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .cache()
                ;
        actors.saveAsTextFile("name.basics.csv");

        JavaRDD<String> films = sc.textFile( fileDir + "title.principals.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .cache()
                ;
        films.saveAsTextFile("title.principals.csv");

        JavaRDD<String> ratings = sc.textFile(fileDir + "title.ratings.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .cache()
                ;
        ratings.saveAsTextFile("title.ratings.csv");

        JavaRDD<String> titleBasics = sc.textFile(fileDir + "title.basics.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .cache()
                ;
        titleBasics.saveAsTextFile("title.basics.csv");
    }
}
