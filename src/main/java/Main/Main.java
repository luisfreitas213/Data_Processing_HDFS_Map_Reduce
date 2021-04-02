package Main;

import MoviesByYear.MoviesByYear;
import MoviesByYear.MoviesByYearTest;
import SecondarySort.SecondarySortBasicDriver;
import ToMerge.ToMerge;
import ToMerge.ToMergeTest;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        try {
            FileUtils.deleteDirectory(new File("hdfs:///MoviesByYear"));
            FileUtils.deleteDirectory(new File("hdfs:///MoviesByYearTest"));
            FileUtils.deleteDirectory(new File("hdfs:///ToMergeTest"));
            FileUtils.deleteDirectory(new File("hdfs:///Output"));
            FileUtils.deleteDirectory(new File("hdfs:///SecondarySort"));
        }finally { }

        ToMerge.tomerge("hdfs:///data/title.ratings.tsv.gz","hdfs:///data/title.basics.tsv.gz");
        MoviesByYear.moviesbyyear("hdfs:///Output");
        SecondarySortBasicDriver.secondarysort("hdfs:///Output");

        /*
        MoviesByYearTest.moviesbyyeartest("MoviesByYear");
        ToMergeTest.tomergetest("Output");
         */
    }
}
