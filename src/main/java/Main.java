import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;
import scala.Tuple7;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    final static private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy HH:mm");

    final private JavaSparkContext sparkContext;

    Main(String path, List<Date> dates) {

        // set spark context
        SparkConf conf = new SparkConf().setAppName("test_app").setMaster("local[*]");
        sparkContext = new JavaSparkContext(conf);

        print(interpolate(parse(path), dates).collect());

        sparkContext.stop();
    }

    private JavaPairRDD<String, Tuple6<Date, Double, Double, Double, Double, Integer>> parse(String path) {
        return this.sparkContext
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

                // maps each pair into (file-name, (time, opening, highest, lowest, closing, volume)
                .map(s -> new Tuple2<>(s._1, s._2.replaceAll(" ", "").trim().split(",")))
                .filter(s -> s._2.length == 7)
                .mapToPair(s -> {
                    Date time = DATE_FORMAT.parse(s._2[0] + " " + s._2[1]);
                    double opening = Double.parseDouble(s._2[2]);
                    double highest = Double.parseDouble(s._2[3]);
                    double lowest = Double.parseDouble(s._2[4]);
                    double closing = Double.parseDouble(s._2[5]);
                    int volume = Integer.parseInt(s._2[6]);
                    Tuple6<Date, Double, Double, Double, Double, Integer> data = new Tuple6<>(time, opening, highest, lowest, closing, volume);
                    return new Tuple2<>(s._1, data);
                });
    }

    private JavaPairRDD<String, List<Tuple3<Date, Double, Double>>> interpolate(JavaPairRDD<String, Tuple6<Date, Double, Double, Double, Double, Integer>> rdd, List<Date> dates) {

        if (dates.size() < 2) throw new IllegalArgumentException("dates.size() should be at least 2");

        // sorting for safety purposes
        dates.sort(Date::compareTo);

        return rdd
                // creates for each file (file-name, [(time, opening, highest, lowest, closing, volume)]) sorted on time
                .mapToPair(s -> new Tuple2<>(s._1, Collections.singletonList(s._2)))
                .reduceByKey(ListUtils::union)
                .mapToPair(s -> {
                    s._2.sort(Comparator.comparing(Tuple6::_1));
                    return new Tuple2<>(s._1, s._2);
                })

                // merges entries which share timestamps
                .mapToPair(s -> {
                    List<Tuple6<Date, Double, Double, Double, Double, Integer>> entries = new LinkedList<>();

                    // iterates through [(time, opening, highest, lowest, closing, volume)]
                    for (int i = 0; i < s._2.size();) {
                        Date current_time = s._2.get(i)._1();   // takes the current timestamp being inspected
                        int count = 0;                          // number of observed entries with current_time as timestamp
                        double sum_openings = 0;                // sums all opening values to afterwards take the mean
                        double highest = Double.MIN_VALUE;      // highest observed highest value
                        double lowest = Double.MAX_VALUE;       // lowest observed lowest value
                        double sum_closing = 0;                 // sums all closing values to afterwards take the mean
                        int sum_volume = 0;

                        // iterates through all entries with timestamp being current_time
                        // updates all observed values accordingly
                        while (i < s._2.size() && s._2.get(i)._1().equals(current_time)) {
                            Tuple6<Date, Double, Double, Double, Double, Integer> entry = s._2.get(i);
                            count++;
                            sum_openings += entry._2();
                            highest = Math.max(highest, entry._3());
                            lowest = Math.min(lowest, entry._4());
                            sum_closing += entry._5();
                            sum_volume += entry._6();
                            i++;
                        }

                        entries.add(new Tuple6<>(current_time, sum_openings / count, highest, lowest, sum_closing / count, sum_volume));
                    }
                    return new Tuple2<>(s._1, entries);
                })

                // adds (when necessary) artificial start and end observations
                // an artificial start is added when the first observation took place after the first queried date
                // in this case, an artificial start node is added at x-time before the first queried date,
                // where x is the difference between the first and second queried date
                // this artifical start node has the same observed values as the first observation
                // a symmetric definition holds for the artificial end
                .mapToPair(s -> {
                    List<Tuple6<Date, Double, Double, Double, Double, Integer>> entries = new LinkedList<>();

                    Tuple6<Date, Double, Double, Double, Double, Integer> first = s._2.get(0);
                    if (first._1().after(dates.get(0))) {
                        Date start_time = new Date(2 * dates.get(0).getTime() - dates.get(1).getTime());
                        Tuple6<Date, Double, Double, Double, Double, Integer> artificial_start = new Tuple6<>(start_time, first._2(), first._3(), first._4(), first._5(), first._6());
                        entries.add(artificial_start);
                    }

                    entries.addAll(s._2);

                    Tuple6<Date, Double, Double, Double, Double, Integer> last = s._2.get(s._2.size()-1);
                    if (last._1().before(dates.get(dates.size()-1))) {
                        Date end_time = new Date(2 * dates.get(dates.size()-1).getTime() - dates.get(dates.size()-2).getTime());
                        Tuple6<Date, Double, Double, Double, Double, Integer> artificial_end = new Tuple6<>(end_time, last._2(), last._3(), last._4(), last._5(), last._6());
                        entries.add(artificial_end);
                    }

                    return new Tuple2<>(s._1, entries);
                })

                // defines an interval for each observation being the time in milliseconds between this and previous observation
                // an exception is made for the first observation, its interval is the time between this and next observation
                // (file-name, [(time, opening, highest, lowest, closing, volume, interval)]) is the new format
                .mapToPair(s -> {
                    LinkedList<Tuple7<Date, Double, Double, Double, Double, Integer, Long>> entries = new LinkedList<>();
                    for (int i = 0; i < s._2.size(); i++) {
                        Tuple6<Date, Double, Double, Double, Double, Integer> entry = s._2.get(i);
                        Long interval = i == 0 ? s._2.get(i+1)._1().getTime() - entry._1().getTime() : entry._1().getTime() - s._2.get(i-1)._1().getTime();
                        entries.add(new Tuple7<>(entry._1(), entry._2(), entry._3(), entry._4(), entry._5(), entry._6(), interval));
                    }
                    return new Tuple2<>(s._1, entries);
                })

                // interpolates to obtain queried dates
                // returns (file-name, [(time, price, sales)])
                // every observation has a time from the queried timestamps
                // price corresponds to the expected price of the stock at this queried point in time
                // sales corresponds to the 'speed of transactions' taking place at this queried point in time
                .mapToPair(s -> {
                    List<Tuple3<Date, Double, Double>> values = new LinkedList<>();

                    int i = 0;
                    for (Date date : dates) {
                        // takes observations prev and next such that prev.time <= date.time < next.time and there are no observations between prev and next
                        while (date.after(s._2.get(i)._1())) i++;
                        Tuple7<Date, Double, Double, Double, Double, Integer, Long> prev = s._2.get(i - 1);
                        Tuple7<Date, Double, Double, Double, Double, Integer, Long> next = s._2.get(i);

                        // takes interpolation value alpha to correspond how close queried date is to the observations
                        // alpha = 0 implies date.time == prev.time and hence date is at the start of the interval defined by next
                        // alpha = 1 implies date.time == next.time (approx) and hence date is at the end of the interval defined by next
                        long prev_time = prev._1().getTime();
                        long cur_time = date.getTime();
                        long next_time = next._1().getTime();
                        double alpha = prev_time == next_time ? 1d : (cur_time - prev_time) / (next_time - prev_time);

                        // takes the price to be the interpolation between the opening and closing price at the observation of next
                        double price = (1-alpha) * next._2() + alpha * next._5();

                        // takes the 'speed of transactions' to be prev.volume / prev.interval at the previous observation
                        // takes the 'speed of transactions' to be next.volume / next.interval at the next observation
                        // interpolates between these two values to form sales
                        double sales = (1-alpha) * prev._6() / prev._7() + alpha * next._6() / next._7();

                        values.add(new Tuple3<>(date, price, sales));
                    }
                    return new Tuple2<>(s._1, values);
                });
    }

    // extremely generic print function
    private void print(Collection output) {
        for (Object tuple : output) System.out.println(tuple.toString());
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/winutils");
        String path = "C:/Users/s161530/Desktop/Data Engineering/Data 2020/202001_Amsterdam_AALB_NoExpiry";

        List<Date> dates = new ArrayList<>();
        try {
            dates.add(DATE_FORMAT.parse("01/02/2020 09:00"));
            dates.add(DATE_FORMAT.parse("01/02/2020 09:05"));
            dates.add(DATE_FORMAT.parse("01/02/2020 09:10"));
            dates.add(DATE_FORMAT.parse("01/02/2020 09:15"));
            dates.add(DATE_FORMAT.parse("01/02/2020 09:20"));
            dates.add(DATE_FORMAT.parse("01/02/2020 09:25"));
        } catch (ParseException e) {
            e.printStackTrace();
        }


        new Main(path, dates);
    }
}