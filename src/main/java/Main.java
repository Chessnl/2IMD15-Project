import org.apache.commons.collections.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        // set spark context
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf conf = new SparkConf().setAppName("test_app").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // define presets
        String path = "C:/Users/s161530/Desktop/Data Engineering/Data 2020/";
        SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");

        List<Tuple2<String, List<Tuple2<Date, BigDecimal>>>> output = sc
                // load all files specified by path, stores as (path-to-file, file-content)
                .wholeTextFiles(path + "*")

                // remove path prefix
                .map(s -> new Tuple2<>(s._1.replaceAll("file:/" + path, ""), s._2))

                // create a new pairs (file-name, line) for each line in file-content
                .flatMapToPair(s -> {
                    List<Tuple2<String, String>> newPairs = new LinkedList<>();
                    for (String line : s._2.split("\n")) {
                        newPairs.add(new Tuple2<>(s._1, line));
                    }
                    return newPairs.iterator();
                })

                // maps each pair into ((file-name, date-string), volume)
                .map(s -> new Tuple2<>(s._1, s._2.split(",")))
                .filter(s -> s._2.length == 7)
                .mapToPair(s -> new Tuple2<>(new Tuple2<>(s._1, s._2[0]), s._2[6].replaceAll(" ", "").trim()))
                .mapToPair(s -> new Tuple2<>(s._1, new BigDecimal(s._2)))

                // adds all volumes from that took place for a certain file on a certain date
                .reduceByKey(BigDecimal::add)

                // maps each pair into (file-name, [(date, volume)])
                .mapToPair(s -> new Tuple2<>(s._1._1, new Tuple2<>(format.parse(s._1._2), s._2)))
                .mapToPair(s -> new Tuple2<>(s._1, Collections.singletonList(s._2)))

                // concatenate all list of pairs from the same file-name and sort this list on date
                .reduceByKey(ListUtils::union)
                .mapToPair(s -> {
                    s._2.sort(Comparator.comparing(dateBigDecimalTuple2 -> dateBigDecimalTuple2._1));
                    return new Tuple2<>(s._1, s._2);
                })

                // return all pairs
                .collect();
        sc.stop();

        // print the collected pairs
        for (Tuple2<String, List<Tuple2<Date, BigDecimal>>> tuple : output) {
            System.out.println(tuple._1);
//            for (Tuple2<Date, BigDecimal> t : tuple._2) {
//                System.out.println(format.format(t._1) + ": " + t._2.toString());
//            }
        }
    }
}