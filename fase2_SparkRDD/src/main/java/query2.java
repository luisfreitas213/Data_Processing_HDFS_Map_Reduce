import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Collections;
import java.util.List;

public class query2 {

    public static void query1(SparkSession spark,Dataset<Row> title_basics, Dataset<Row> title_ratings,Dataset<Row> title_principal,Dataset<Row> name_basics){
        System.out.println("--------------------------------Query 1--------------------------------");
        //Base:Nome, idade, número de títulos em que participou, intervalo de anos de atividade eclassificação média dos títulos em que participa.

        Dataset<Row> movies = title_basics.join(title_principal, title_basics.col("tconst2").equalTo(title_principal.col("tconst")), "inner");
        movies=movies.drop("tconst2");
        movies=movies.drop("ordering");
        movies=movies.drop("category");
        movies=movies.drop("job");
        movies=movies.drop("characters");

        Dataset<Row> query1 = movies.join(name_basics, movies.col("nconst").equalTo(name_basics.col("nconst2")),"inner");
        query1=query1.drop("nconst2");

        query1=query1.join(title_ratings, query1.col("tconst").equalTo(title_ratings.col("tconst2")),"inner");
        query1=query1.drop("tconst2");

        query1.createOrReplaceTempView("query1");
        query1 = spark.sql("select nconst, min(startYear) as first, max(startYear) as last, avg(averageRating) as averageRating," +
                " count(*) as n_titles  from query1 group by nconst");

        query1=query1.join(name_basics, query1.col("nconst").equalTo(name_basics.col("nconst2")),"inner");
        query1.createOrReplaceTempView("query1");

        query1 = spark.sql("select primaryName, n_titles, first, last, averageRating,   " +
                "case when deathYear == '\\\\N' then  YEAR(current_date()) - birthYear " +
                "else deathYear-birthYear " +
                "end as age " +
                "from query1 order by n_titles desc");

        query1.show();
    }

    public static void query2(SparkSession spark,Dataset<Row> title_basics, Dataset<Row> title_ratings,Dataset<Row> title_principal,Dataset<Row> name_basics){
        System.out.println("--------------------------------Query 2--------------------------------");
        //Hits:Top 10 dos títulos mais bem classificados em que participo.

        Dataset<Row> movies = title_basics.join(title_principal, title_basics.col("tconst2").equalTo(title_principal.col("tconst")), "inner");
        movies=movies.drop("tconst2");
        movies=movies.drop("ordering");
        movies=movies.drop("category");
        movies=movies.drop("job");
        movies=movies.drop("characters");
        movies=movies.drop("startYear");

        Dataset<Row> query2 = movies.join(name_basics, movies.col("nconst").equalTo(name_basics.col("nconst2")),"inner");
        query2=query2.drop("nconst2");
        query2=query2.drop("deathYear");
        query2=query2.drop("birthYear");

        query2=query2.join(title_ratings, query2.col("tconst").equalTo(title_ratings.col("tconst2")),"inner");
        query2=query2.drop("tconst2");

        query2.createOrReplaceTempView("query2");

        query2 = spark.sql("select * from ( select nconst, primaryName, tconst, primaryTitle, averageRating, " +
                " row_number() over (partition by nconst order by averageRating desc) as title_actor_rank " +
                " from query2) ranks " +
                " where title_actor_rank <= 10");

        query2=query2.drop("tconst");
        query2=query2.drop("nconst");
        query2.show();
    }

    public static void query3(SparkSession spark,Dataset<Row> title_ratings,Dataset<Row> title_principal,Dataset<Row> name_basics){
        System.out.println("--------------------------------Query 3--------------------------------");
        //Generation:Nomes do top 10 de atores da mesma geração (i.e., que nasceram na mesma década).

        Dataset<Row> movies = title_ratings.join(title_principal, title_ratings.col("tconst2").equalTo(title_principal.col("tconst")), "inner");
        movies=movies.drop("tconst2");
        movies=movies.drop("numVotes");
        movies=movies.drop("ordering");
        movies=movies.drop("job");
        movies=movies.drop("category");
        movies=movies.drop("characters");

        movies.createOrReplaceTempView("movies");

        movies = spark.sql("select nconst, avg(averageRating) as averageRating from movies group by nconst");

        Dataset<Row> query3 = movies.join(name_basics, movies.col("nconst").equalTo(name_basics.col("nconst2")),"inner");
        query3=query3.drop("nconst2");
        query3=query3.drop("deathYear");

        query3.createOrReplaceTempView("query3");

        query3 = spark.sql("select * from ( select SUBSTRING(birthYear, 0, CHAR_LENGTH(birthYear)-1) as decade, primaryName, averageRating, " +
                " row_number() over (partition by SUBSTRING(birthYear, 0, CHAR_LENGTH(birthYear)-1) order by averageRating desc) as actor_rank " +
                " from query3) ranks " +
                " where actor_rank <= 10 order by decade desc, actor_rank desc");

        query3.show();
    }

    public static void query4(SparkSession spark,Dataset<Row> name_basics, Dataset<Row> title_principal){
        System.out.println("--------------------------------Query 4--------------------------------");
        //Friends:Conjunto de colaboradores de cada ator (i.e., outros atores que participaram nosmesmos títulos).

        Dataset<Row> movies = name_basics.join(title_principal, name_basics.col("nconst2").equalTo(title_principal.col("nconst")), "inner");
        movies=movies.drop("birthYear");
        movies=movies.drop("deathYear");
        movies=movies.drop("primaryProfession");
        movies=movies.drop("knownForTitles");
        movies=movies.drop("ordering");
        movies=movies.drop("category");
        movies=movies.drop("job");
        movies=movies.drop("characters");
        movies=movies.drop("nconst2");

        movies.createOrReplaceTempView("movies");

        Dataset<Row> query4 = spark.sql("SELECT y.*, x.nconst as friend FROM movies x" +
                "  JOIN movies y ON y.tconst = x.tconst" +
                "   AND y.nconst <> x.nconst" +
                " order by x.tconst");

        query4=query4.drop("tconst");

        query4 = name_basics.join(query4, name_basics.col("nconst2").equalTo(query4.col("friend")), "inner");
        query4=query4.drop("birthYear");
        query4=query4.drop("deathYear");
        query4=query4.drop("primaryProfession");
        query4=query4.drop("knownForTitles");
        query4=query4.drop("nconst");
        query4=query4.drop("nconst2");
        query4=query4.drop("friend");
        query4.show(100,false);

    }

    public static void main(String[] args) throws Exception {

        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.OFF);
        }

        SparkSession spark = SparkSession.builder()
                .appName("example")
                .master("local")
                .getOrCreate();

        Dataset<Row> title_basics = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("C:\\Users\\jpmrs\\Downloads\\title.basics.tsv.bz2");

        Dataset<Row> title_ratings = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("C:\\Users\\jpmrs\\Downloads\\title.ratings.tsv.bz2");

        Dataset<Row> title_principal = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("C:\\Users\\jpmrs\\Downloads\\title.principals.tsv.bz2");

        Dataset<Row> name_basics = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv("C:\\Users\\jpmrs\\Downloads\\name.basics.tsv.bz2");

        System.out.println("--------------------------------Schema do title_basics--------------------------------");
        title_basics.printSchema();
        System.out.println("--------------------------------Schema do title_ratings--------------------------------");
        title_ratings.printSchema();
        System.out.println("--------------------------------Schema do title_principal--------------------------------");
        title_principal.printSchema();
        System.out.println("--------------------------------Schema do name_basics--------------------------------");
        name_basics.printSchema();

        title_basics.createOrReplaceTempView("title_basics");
        title_ratings.createOrReplaceTempView("title_ratings");
        title_principal.createOrReplaceTempView("title_principal");
        name_basics.createOrReplaceTempView("name_basics");

        title_basics = title_basics.select(title_basics.col("tconst").alias("tconst2"),
                title_basics.col("startYear"),
                title_basics.col("primaryTitle"));

        title_ratings= title_ratings.select(title_ratings.col("tconst").alias("tconst2"),
                title_ratings.col("averageRating"));

        name_basics = spark.sql("select * from name_basics where primaryProfession='actress' or  primaryProfession='actor' ");

        name_basics = name_basics.select(name_basics.col("nconst").alias("nconst2"),
                name_basics.col("primaryName"),
                name_basics.col("birthYear"),
                name_basics.col("deathYear"));

        name_basics.createOrReplaceTempView("name_basics");

        query1(spark, title_basics,  title_ratings,title_principal, name_basics);

        query2(spark, title_basics,  title_ratings,title_principal, name_basics);

        query3(spark, title_ratings,title_principal, name_basics);

        query4(spark, name_basics, title_principal);
    }

}
