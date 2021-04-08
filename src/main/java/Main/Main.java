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


        ToMerge.tomerge();
        MoviesByYear.moviesbyyear("hdfs:///Output");
        SecondarySortBasicDriver.secondarysort("hdfs:///Output");

        /*
        MoviesByYearTest.moviesbyyeartest("MoviesByYear");
        ToMergeTest.tomergetest("Output");
         */
    }
}
