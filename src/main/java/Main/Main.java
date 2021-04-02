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
            FileUtils.deleteDirectory(new File("MoviesByYear"));
            FileUtils.deleteDirectory(new File("MoviesByYearTest"));
            FileUtils.deleteDirectory(new File("ToMergeTest"));
            FileUtils.deleteDirectory(new File("Output"));
        }finally { }

        ToMerge.tomerge("Data/title.ratings.tsv.gz","Data/title.basics.tsv.gz");
        MoviesByYear.moviesbyyear("Output");
        SecondarySortBasicDriver.secondarysort("Output");
        MoviesByYearTest.moviesbyyeartest("MoviesByYear");
        ToMergeTest.tomergetest("Output");

        //FileUtils.deleteDirectory(new File("Output"));
    }
}
