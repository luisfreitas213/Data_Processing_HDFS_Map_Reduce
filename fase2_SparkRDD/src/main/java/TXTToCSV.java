import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TXTToCSV {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("g4spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final String fileDir = "file:////home/mcmaia/GGCD/dados/mini/";

        sc.textFile(fileDir + "name.basics.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .saveAsTextFile("name.basics.csv");

        sc.textFile( fileDir + "title.principals.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .saveAsTextFile("title.principals.csv");

        sc.textFile(fileDir + "title.ratings.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .saveAsTextFile("title.ratings.csv");

        sc.textFile(fileDir + "title.basics.tsv.bz2")
                .map(l -> l.replace("\t", ";"))
                .saveAsTextFile("title.basics.csv");
    }
}
