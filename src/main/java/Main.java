import Correlations.CorrelationFunction;
import Correlations.MutualInformationCorrelation;
import Correlations.PearsonCorrelation;
import org.apache.commons.collections.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;
import scala.Tuple7;

import java.awt.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

import javax.swing.*;

public class Main {

    final static private String DATE_FORMAT = "MM/dd/yyyy-HH:mm";

    final private JavaSparkContext sparkContext;
    private static final boolean DEBUGGING = false;

    // Choose a correlation function
    private CorrelationFunction correlationFunction = new PearsonCorrelation();
//    private CorrelationFunction correlationFunction = new MutualInformationCorrelation();

    Main(String path, String source, List<Date> dates, int minPartitions) {
        // set spark context
        SparkConf conf = new SparkConf().setAppName("test_app").setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1");
        sparkContext = new JavaSparkContext(conf);

        if (dates.size() < 2) throw new IllegalArgumentException("dates.size() should be at least 2");

        // sorting for safety purposes
        dates.sort(Date::compareTo);

        // For each stock, a list of time and value combinations to compare
        JavaPairRDD<String, List<Tuple2<Date, Double>>> timeSeries = prepareData(
                parse(path, source, dates.get(0), dates.get(dates.size() - 1), minPartitions), dates
        );

        if (DEBUGGING) {
            // For debugging, plot the values
            List<Tuple2<String, List<Tuple2<Date, Double>>>> collected = timeSeries.collect();
            plot(collected);
        }

        // Compare all two stocks against each other by applying the correlation function on each
        JavaPairRDD<Tuple2<String, String>, Double> correlations = calculateCorrelations(timeSeries, correlationFunction);

        // Save the output correlation pairs to a file
        correlations.coalesce(1).saveAsTextFile(path + "000000_OUTPUT");

        if (DEBUGGING) {
            // Filter out the combinations that have a high correlation only
            JavaPairRDD<Tuple2<String, String>, Double> highCorrelations = filterHighCorrelations(correlations);

            // Print the high correlations
            List<Tuple2<Tuple2<String, String>, Double>> highCorrelationsCollected = highCorrelations.collect();
            for (Tuple2<Tuple2<String, String>, Double> highCorrelationEntry : highCorrelationsCollected) {
                System.out.println("High correlation of " + highCorrelationEntry._2() + " between "
                        + highCorrelationEntry._1._1() + " and " + highCorrelationEntry._1._2() + ".");
            }
        }

        sparkContext.stop();
    }

    /**
     * (stockName, [(time, opening, highest, lowest, closing, volume)])
     *
     * @param path      location of stored data
     * @param source    sub-string of data that is matched with
     * @param startDate only considers observations after startDate
     * @param endDate   only considers observations before startDate
     * @return (stockName, [ ( time, opening, highest, lowest, closing, volume)])
     */
    private JavaPairRDD<String, List<Tuple6<Date, Double, Double, Double, Double, Long>>> parse(String path, String source, Date startDate, Date endDate, int minPartitions) {
        // Parse start and end yearMonth
        SimpleDateFormat ymf = new SimpleDateFormat("yyyyMM");
        int startYearMonth = Integer.parseInt(ymf.format(startDate));
        int endYearMonth = Integer.parseInt(ymf.format(endDate));

        // Build path filter
        StringJoiner dates = new StringJoiner(",");
        int d = startYearMonth;
        while (d <= endYearMonth) {
            dates.add(String.valueOf(d));
            d++;
            if (d % 13 == 0) d = (d / 100 + 1) * 100 + 1;
        }

        // creates for each file (stockName, [(time, opening, highest, lowest, closing, volume)])
        return this.sparkContext
                // load all files specified by path, stores as (path-to-file, file-content)
                .wholeTextFiles(path + "{" + dates.toString() + "}_" + source + "_*", minPartitions)


                .mapToPair(s -> {
                    // Obtain stockName from filename
                    String stockName = s._1.replaceAll("file:/" + path, "").split("_")[2];

                    // Process lines in the file
                    List<Tuple6<Date, Double, Double, Double, Double, Long>> observations = new LinkedList<>();
                    String[] lines = s._2.split("\\r?\\n");
                    SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
                    int count = 0;
                    Date time = format.parse("00/00/0000-00:00");
                    double opening = 0;
                    double highest = Double.MIN_VALUE;
                    double lowest = Double.MAX_VALUE;
                    double closing = 0;
                    long volume = 0;

                    // creates [(time, opening, highest, lowest, closing, volume)] for the input file
                    // merges observations which share a timestamp
                    for (String line : lines) {
                        if (line.isEmpty()) continue;
                        String[] entries = line.replaceAll("\\s+", "").split(",");
                        // Parse Date
                        Date newTime = format.parse(entries[0].trim() + "-" + entries[1].trim());
                        // Skip entries before startDate whilst end processing if entry date is after endDate
                        if (newTime.compareTo(startDate) < 0) continue;
                        if (newTime.compareTo(endDate) > 0) break;
                        // Process rest of the pair only if time is within the desired bound
                        if (!newTime.equals(time)) {
                            if (count > 0) {
                                // Add the last processed pair to results
                                observations.add(new Tuple6<>(
                                        time,
                                        opening / count, // Take the mean
                                        highest, lowest,
                                        closing / count, // Take the mean
                                        volume
                                ));
                            }
                            // Parse new pair
                            time = newTime;
                            opening = Double.parseDouble(entries[2]);
                            highest = Double.parseDouble(entries[3]);
                            lowest = Double.parseDouble(entries[4]);
                            closing = Double.parseDouble(entries[5]);
                            volume = Double.valueOf(entries[6]).longValue();
                            count = 1;
                        } else {
                            // Entry at same timestamp
                            opening += Double.parseDouble(entries[2]);
                            highest = Math.max(highest, Double.parseDouble(entries[3]));
                            lowest = Math.min(lowest, Double.parseDouble(entries[4]));
                            closing += Double.parseDouble(entries[5]);
                            volume += Long.parseLong(entries[6]);
                            count++;
                        }
                    }
                    // Add the last Pair
                    if (count > 0) {
                        observations.add(new Tuple6<>(
                                time,
                                opening / count, // Take the mean
                                highest, lowest,
                                closing / count, // Take the mean
                                volume
                        ));
                    }

                    return new Tuple2<>(stockName, observations);
                });
    }

    /**
     * Given a set of (stockName, [(time, opening, highest, lowest, closing, volume)]), calculates an estimate of the prices
     * at given dates. Returns for each stockName the (percentage) change in price between dates[i] and dates[i-1].
     *
     * @param rdd   (stockName, [(time, opening, highest, lowest, closing, volume)])
     * @param dates [time]
     * @return (stockName, [ ( time, price - difference)])
     */
    private JavaPairRDD<String, List<Tuple2<Date, Double>>> prepareData(JavaPairRDD<String, List<Tuple6<Date, Double, Double, Double, Double, Long>>> rdd, List<Date> dates) {
        return rdd
                // creates for each stock (stock-name, [(time, opening, highest, lowest, closing, volume)]) sorted on time
                .reduceByKey(ListUtils::union)
                .map(s -> {
                    s._2.sort(Comparator.comparing(Tuple6::_1));
                    return new Tuple2<>(s._1, s._2);
                })

                // only consider stocks which had at least 10 observations
                .filter(s -> s._2.size() >= 10)

                // prepares data for interpolation
                .mapToPair(s -> {
                    // adds (when necessary) artificial start and end observations
                    // an artificial start is added when the first observation took place after the first queried date
                    // in this case, an artificial start node is added at x-time before the first queried date,
                    // where x is the difference between the first and second queried date
                    // this artifical start node has the same observed values as the first observation
                    // a symmetric definition holds for the artificial end
                    List<Tuple6<Date, Double, Double, Double, Double, Long>> observations = new LinkedList<>();

                    Tuple6<Date, Double, Double, Double, Double, Long> first = s._2.get(0);
                    if (first._1().after(dates.get(0))) {
                        Date start_time = new Date(2 * dates.get(0).getTime() - dates.get(1).getTime());
                        Tuple6<Date, Double, Double, Double, Double, Long> artificial_start = new Tuple6<>(start_time, first._2(), first._3(), first._4(), first._5(), first._6());
                        observations.add(artificial_start);
                    }

                    observations.addAll(s._2);

                    Tuple6<Date, Double, Double, Double, Double, Long> last = s._2.get(s._2.size() - 1);
                    if (last._1().before(dates.get(dates.size() - 1))) {
                        Date end_time = new Date(2 * dates.get(dates.size() - 1).getTime() - dates.get(dates.size() - 2).getTime());
                        Tuple6<Date, Double, Double, Double, Double, Long> artificial_end = new Tuple6<>(end_time, last._2(), last._3(), last._4(), last._5(), last._6());
                        observations.add(artificial_end);
                    }

                    // defines an interval for each observation being the time in milliseconds between this and previous observation
                    // an exception is made for the first observation, its interval is the time between this and next observation
                    // (file-name, [(time, opening, highest, lowest, closing, volume, interval)]) is the new format
                    LinkedList<Tuple7<Date, Double, Double, Double, Double, Long, Long>> entries = new LinkedList<>();
                    for (int i = 0; i < observations.size(); i++) {
                        Tuple6<Date, Double, Double, Double, Double, Long> entry = observations.get(i);
                        Long interval = i == 0 ? observations.get(i + 1)._1().getTime() - entry._1().getTime() : entry._1().getTime() - observations.get(i - 1)._1().getTime();
                        entries.add(new Tuple7<>(entry._1(), entry._2(), entry._3(), entry._4(), entry._5(), entry._6(), interval));
                    }

                    // interpolates to obtain queried dates
                    // returns (file-name, [(time, price)])
                    // every observation has a time from the queried timestamps
                    // price corresponds to the expected price of the stock at this queried point in time
                    List<Tuple2<Date, Double>> values = new LinkedList<>();

                    int i = 0;
                    for (Date date : dates) {
                        // takes observations prev and next such that prev.time <= date.time < next.time and there are no observations between prev and next
                        while (date.after(entries.get(i)._1())) i++;
                        Tuple7<Date, Double, Double, Double, Double, Long, Long> prev = entries.get(i - 1);
                        Tuple7<Date, Double, Double, Double, Double, Long, Long> next = entries.get(i);

                        // takes interpolation value alpha to correspond how close queried date is to the observations
                        // alpha = 0 implies date.time == prev.time and hence date is at the start of the interval defined by next
                        // alpha = 1 implies date.time == next.time (approx) and hence date is at the end of the interval defined by next
                        long prev_time = prev._1().getTime();
                        long cur_time = date.getTime();
                        long next_time = next._1().getTime();
                        double alpha = prev_time == next_time ? 1d : (cur_time - prev_time) / (next_time - prev_time);

                        // takes the price to be the interpolation between the opening and closing price at the observation of next
                        double price = (1 - alpha) * next._2() + alpha * next._5();

                        values.add(new Tuple2<>(date, price));
                    }

                    // calculates the price difference as a percentage
                    List<Tuple2<Date, Double>> data = new LinkedList<>();
                    for (int j = 1; j < values.size(); j++) {
                        Tuple2<Date, Double> current = values.get(j);
                        Tuple2<Date, Double> prev = values.get(j - 1);

                        // Price change as percentage of old price
                        double rateOfChange = (current._2() - prev._2()) / prev._2();

                        data.add(new Tuple2<>(current._1(), rateOfChange));
                    }

                    return new Tuple2<>(s._1, data);
                });
    }

    /**
     * Compare all two stocks against each other by applying the correlation function on each
     *
     * @param timeSeries
     * @param correlationFunction
     * @return
     */
    private JavaPairRDD<Tuple2<String, String>, Double> calculateCorrelations(
            JavaPairRDD<String, List<Tuple2<Date, Double>>> timeSeries,
            CorrelationFunction correlationFunction
    ) {
        // get the cartesian product of timeSeries so we have a Tuple2 for every stock pair
        return timeSeries.cartesian(timeSeries)

                // filter out the tuples with the same stock twice.
                .filter(s -> s._1._1.compareTo(s._2._1) > 0)

                // call the getCorrelation function on the stock pairs.
                .mapToPair(s -> new Tuple2<>(new Tuple2<>(s._1._1, s._2._1),
                        correlationFunction.getCorrelation(s._1._2, s._2._2)));
    }

    private JavaPairRDD<Tuple2<String, String>, Double> filterHighCorrelations(JavaPairRDD<Tuple2<String, String>, Double> correlations) {
        // TODO Filter out the combinations that have a high correlation only

        return correlations.filter(correlation -> {
            double value = correlation._2;
            double threshold = 0.8; // TODO refine
            return value > threshold;
        });
    }

    // simple plot function
    private void plot(List<Tuple2<String, List<Tuple2<Date, Double>>>> data) {
        // Create Dataset to Plot
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        for (Tuple2<String, List<Tuple2<Date, Double>>> stock : data) {
            TimeSeries series = new TimeSeries(stock._1);
            for (Tuple2<Date, Double> entry : stock._2) {
                series.add(new Minute(entry._1()), entry._2());
            }
            dataset.addSeries(series);
        }

        // Create UI
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Data Engineering",
                "time",
                "value",
                dataset,
                true,
                true,
                true
        );
        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE);

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        // Lines with dots
        renderer.setSeriesLinesVisible(0, true);
        // Tooltip for dots
        renderer.setDefaultToolTipGenerator(new StandardXYToolTipGenerator());
        // Assign renderer to plot
        plot.setRenderer(renderer);

        // Create plot
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setMouseWheelEnabled(true);
        chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        chartPanel.setBackground(Color.WHITE);
        // Tooltip settings
        chartPanel.setInitialDelay(0);
        chartPanel.setReshowDelay(0);
        chartPanel.setDismissDelay(Integer.MAX_VALUE);

        // JFrame
        JFrame frame = new JFrame();
        frame.setSize(1400, 600);
        frame.add(chartPanel);
        frame.pack();
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    // @TODO should prepare better dates
    // only create times during daytime on workdays
    static List<Date> generateDates(Date start, Date end, Long interval) {
        LinkedList<Date> dates = new LinkedList<>();
        Date cur = start;
        while (cur.before(end)) {
            dates.add(cur);
            cur = new Date(cur.getTime() + interval);
        }
        dates.add(end);

        return dates;
    }

    public static void main(String[] args) {

        // Load the config
        Properties config = new Properties();
        try {
            config.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            System.out.println("Please copy config.properties.example to config.properties and fill it in");
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("Reading data from " + config.getProperty("data_path"));
        System.out.println("Using Hadoop directory " + config.getProperty("hadoop_path"));
        System.out.println("Minimum partitions when reading files: " + config.getProperty("min_partitions"));
        System.out.println("Matching with stocks " + config.getProperty("data_match"));

        System.setProperty("hadoop.home.dir", config.getProperty("hadoop_path"));
        String path = config.getProperty("data_path");
        int minPartitions = Integer.parseInt(config.getProperty("min_partitions"));
        String source = config.getProperty("data_match"); // only considers stocks that contain `source` as a sub-string

        List<Date> dates = null;
        try {
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
            dates = generateDates(format.parse("01/01/2020-00:00"), format.parse("02/10/2020-00:00"), 86400000L);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        new Main(path, source, dates, minPartitions);
    }
}