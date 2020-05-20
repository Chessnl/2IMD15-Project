import Aggregations.*;
import Correlations.CorrelationFunction;
import Correlations.MutualInformationCorrelation;
import Correlations.PearsonCorrelation;
import Correlations.TotalCorrelation;
import org.apache.commons.collections.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import scala.Tuple2;
import scala.Tuple6;

import javax.swing.*;
import java.awt.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    final static private String DATE_FORMAT = "MM/dd/yyyy-HH:mm";

    private static final boolean DEBUGGING = false;
    final private SparkSession sparkSession;

    // Choose a correlation function
    private final CorrelationFunction pearsonCorrelationFunction = new PearsonCorrelation();
    private final CorrelationFunction mutualInformationFunction = new MutualInformationCorrelation();
    private final CorrelationFunction totalCorrelationFunction = new TotalCorrelation();

    // Choose an aggregation function
    private final AggregationFunction averageAggregationFunction = new AverageAggregation();
    private final IdentityAggregation identityAggregationFunction = new IdentityAggregation();
    private final MaxAggregation maxAggregationFunction = new MaxAggregation();
    private final MinAggregation minAggregationFunction = new MinAggregation();
    private final NormalAverageAggregation normalAverageAggregationFunction = new NormalAverageAggregation();
    private final NormalizationAggregation normalizationAggregationFunction = new NormalizationAggregation();

    Main(String path, String outputPath, String source, Date start_date, Date end_date,
         String masterNode, String sparkDriver,
         int minPartitions, int numSegments, int dimensions, boolean server, String[] exclusions) {

        // Define a new output folder based on date and time and copy the current config to it
        String outputFolder = "out_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

        // Copy config to output folder
        copyConfig(outputPath, outputFolder, server);

        // Create sparkSession
        if (server) {
            sparkSession = SparkSession.builder().appName("Main").getOrCreate();
        } else {
            SparkConf conf = new SparkConf().setAppName("test_app").setMaster(masterNode).set("spark.driver.bindAddress", sparkDriver);
            sparkSession = SparkSession.builder().appName("Main").config(conf).getOrCreate();
        }

        // Generate dates
        List<Date> dates = generateDates(start_date, end_date);
        if (dates.size() < 2) throw new IllegalArgumentException("dates.size() should be at least 2");

        // Read in data: For each stock, a list of observations
        JavaPairRDD<String, List<Tuple6<Date, Double, Double, Double, Double, Long>>> parsed =
                parse(path, source, start_date, end_date, minPartitions, exclusions);

        // Prepare the data: Extract one preprocessed time-series for each stock
        JavaPairRDD<String, List<Double>> timeSeries = prepareData(parsed, dates);

        if (DEBUGGING) {
            // For debugging, plot the values
            plot(timeSeries.collect());
        }

        // compute the PearsonCorrelation Function
        JavaPairRDD<List<String>, Double> pearsonCorrelations =
                calculateCorrelations(timeSeries, pearsonCorrelationFunction, averageAggregationFunction, true, numSegments, dimensions);
        saveCorrelationResultsToFile(pearsonCorrelations, "Pearson", server, outputPath, outputFolder);

        // compute the MutualInformation correlation
        JavaPairRDD<List<String>, Double> mutualCorrelations =
                calculateCorrelations(timeSeries, mutualInformationFunction, averageAggregationFunction, true, numSegments, dimensions);
        saveCorrelationResultsToFile(mutualCorrelations, "MutualInformation", server, outputPath, outputFolder);

        // compute the TotalCorrelation
        JavaPairRDD<List<String>, Double> totalCorrelation =
                calculateCorrelations(timeSeries, totalCorrelationFunction, identityAggregationFunction, false, numSegments, dimensions);
        saveCorrelationResultsToFile(totalCorrelation, "TotalCorrelation", server, outputPath, outputFolder);

        sparkSession.stop();
    }

    /**
     * Copy the config.properties file to an output folder
     * @param outputPath
     * @param server
     */
    private static void copyConfig(String outputPath, String outputFolder, boolean server) {
        if (!server) {
            try {
                Files.createDirectories(Paths.get(outputPath, outputFolder));
                Files.copy(Paths.get("config.properties"), Paths.get(outputPath, outputFolder, "config.properties"));
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    private void saveCorrelationResultsToFile(JavaPairRDD<List<String>, Double> result,
                                              String name, Boolean server, String outputPath, String outputFolder
    ) {
        if (server) {
            // On the server do not coalesce and do not use java Paths to support hdfs
            result.saveAsTextFile(outputPath + outputFolder + name);
        } else {
            // On a local system coalesce before writing to file
            result.coalesce(1).saveAsTextFile(
                    Paths.get(outputPath, outputFolder, name).toUri().getPath());
        }
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
    private JavaPairRDD<String, List<Tuple6<Date, Double, Double, Double, Double, Long>>> parse(
            String path, String source, Date startDate, Date endDate, int minPartitions, String[] exclusions
    ) {
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
        return this.sparkSession.sparkContext()
                // load all files specified by path, stores as (path-to-file, file-content)
                .wholeTextFiles(path + "{" + dates.toString() + "}_" + source + "_*", minPartitions).toJavaRDD()

                // Filter out ignored files
                .filter(s -> Arrays.stream(exclusions).noneMatch(s._1::contains))

                .mapToPair(s -> {
                    // Obtain stockName from filename
                    String stockName = s._1.replaceAll("file:/" + path, "").replaceAll("_NoExpiry.txt", "").split("_", 2)[1];

                    // Process lines in the file
                    List<Tuple6<Date, Double, Double, Double, Double, Long>> observations = new ArrayList<>();
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
    private JavaPairRDD<String, List<Double>> prepareData(
            JavaPairRDD<String, List<Tuple6<Date, Double, Double, Double, Double, Long>>> rdd, List<Date> dates
    ) {
        return rdd
                // creates for each stock (stock-name, [(time, opening, highest, lowest, closing, volume)]) sorted on time
                .reduceByKey(ListUtils::union)
                // only consider stocks which had at least 10 observations
                .filter(s -> s._2.size() >= 10)

                // interpolates data
                .mapToPair(s -> {
                    // interpolates to obtain queried dates
                    // returns (file-name, [(time, price)])
                    // every observation has a time from the queried timestamps
                    // price corresponds to the expected price of the stock at this queried point in time

                    // Sort observations on time for safety
                    s._2.sort(Comparator.comparing(Tuple6::_1));

                    // Interpolate
                    List<Double> prices = new ArrayList<>(dates.size());
                    int i = 0;
                    for (Date date : dates) {
                        // takes observations prev and next such that prev.time <= date.time < next.time and there are no observations between prev and next
                        while (i < s._2.size() && date.after(s._2.get(i)._1())) i++;

                        if (i == 0) { // takes first observation if query time is before the first observation
                            prices.add(s._2.get(0)._5());
                        } else if (i == s._2.size()) { // take last observation if query time is after the last observation
                            prices.add(s._2.get(s._2.size() - 1)._5());
                        } else {
                            Tuple6<Date, Double, Double, Double, Double, Long> prev = s._2.get(i - 1);
                            Tuple6<Date, Double, Double, Double, Double, Long> next = s._2.get(i);

                            // takes interpolation value alpha to correspond how close queried date is to the observations
                            // alpha = 0 implies date.time == prev.time and hence date is at the start of the interval defined by next
                            // alpha = 1 implies date.time == next.time (approx) and hence date is at the end of the interval defined by next
                            long prev_time = prev._1().getTime();
                            long cur_time = date.getTime();
                            long next_time = next._1().getTime();
                            double alpha = prev_time == next_time ? 1d : (cur_time - prev_time) / (next_time - prev_time);

                            // takes the price to be the interpolation between the opening and closing price at the observation of next
                            double price = (1 - alpha) * next._2() + alpha * next._5();

                            prices.add(price);
                        }
                    }
                    return new Tuple2<>(s._1, prices);
                });
    }

    /**
     * Compare all two stocks against each other by applying the correlation function on each
     *
     * @param timeSeries
     * @param correlationFunction
     * @return
     */
    private JavaPairRDD<List<String>, Double> calculateCorrelations(
            JavaPairRDD<String, List<Double>> timeSeries,
            CorrelationFunction correlationFunction,
            AggregationFunction aggregationFunction,
            boolean reduceDimensionality,
            int numSegments,
            int dimensions
    ) {
        JavaPairRDD<Integer, List<Tuple2<String, List<Double>>>> keyed = timeSeries.mapToPair(s -> {
            // Double mod as Java mods to (-numSegments, numSegments), but we want only [0, numSegments).
            int hash = ((s._1.hashCode() % numSegments) + numSegments) % numSegments;
            List<Tuple2<String, List<Double>>> value = new ArrayList<>();
            value.add(new Tuple2<>(s._1, s._2));
            return new Tuple2<>(hash, value);
        });

        JavaPairRDD<Integer, List<Tuple2<String, List<Double>>>> segments = keyed.reduceByKey(ListUtils::union);

        JavaPairRDD<Integer, // Bucket number
                List< // List of segments, one of each dimension (List.size = dimentions)
                        List<Tuple2< // List of stocks in this segment (List.size = numStocks / numSegments (on average))
                                String, // Stock name
                                List<Double> // Stock timeseries
                                >>>> buckets = segments.flatMapToPair(s -> {
            int segmentId = s._1; // The hash this segment is made of
            System.out.println("Segment " + segmentId + " contains " + s._2.size() + " stocks.");
            List<Tuple2<Integer, List<List<Tuple2<String, List<Double>>>>>> bucketAssignments = new ArrayList<>();
            for (int onDimension = 0; onDimension < dimensions; onDimension++) { // For every axis this stock is along
                // Add it to all values for the other dimensions
                for (int bucketNumber : getBucketNumbersForIdAlongDimension(segmentId, onDimension, dimensions, numSegments)) {
                    List<List<Tuple2<String, List<Double>>>> bucketAssignment = new ArrayList<>();
                    bucketAssignment.add(s._2);
                    bucketAssignments.add(new Tuple2<>(bucketNumber, bucketAssignment));
                }
            }
            return bucketAssignments.iterator();
        }).reduceByKey(ListUtils::union);

        // Only consider buckets with as many values as dimensions in case not every segment has at least one stock
        buckets = buckets.filter(bucket -> bucket._2.size() == dimensions);

        return buckets
                .flatMapToPair(s -> {
                    int bucketId = s._1;
                    List<List<Tuple2<String, List<Double>>>> segmentsInBucket = s._2;
                    List<Tuple2<List<String>, Double>> out = new ArrayList<>();
                    return compareAllPairs(new ArrayList<>(), segmentsInBucket, correlationFunction, aggregationFunction, reduceDimensionality).iterator();
                });
    }

    private static List<Tuple2<List<String>, Double>> compareAllPairs(
            List<Tuple2<String, List<Double>>> compareThese,
            List<List<Tuple2<String, List<Double>>>> toAllCombinationsOfThese,
            CorrelationFunction correlationFunction,
            AggregationFunction aggregationFunction,
            boolean reduceDimensionality) {
        List<Tuple2<List<String>, Double>> out = new ArrayList<>();
        // NB: All double comparisons are done, but not against itself

        if (toAllCombinationsOfThese.isEmpty()) {
            // Done recursing, compute correlation

            List<String> stockNames = compareThese.stream().map(stock -> stock._1).collect(Collectors.toCollection(ArrayList::new));
            if (stockNames.stream().distinct().count() == stockNames.size()) {
                // All stocks have a different name

                // Aggregate (with or without reducing dimensionality)
                List<Tuple2<String, List<Double>>> aggregated = reduceDimensionality ?
                        reduceDimensionality(compareThese, aggregationFunction) :
                        aggregationFunction.aggregate(compareThese);

                // Calculate correlation
                double correlation = correlationFunction.getCorrelation(aggregated);

                out.add(new Tuple2<>(stockNames, correlation));
            }
        } else {
            List<Tuple2<String, List<Double>>> segment = toAllCombinationsOfThese.get(0);
            toAllCombinationsOfThese.remove(segment);
            for (Tuple2<String, List<Double>> stock : segment) {
                compareThese.add(stock);
                out.addAll(compareAllPairs(compareThese, toAllCombinationsOfThese, correlationFunction, aggregationFunction, reduceDimensionality));
                compareThese.remove(stock);
            }
            toAllCombinationsOfThese.add(segment);
        }
        return out;
    }

    /**
     * Reduce the dimensionality of the to be compared pairs to two dimensions using some aggregation function
     * @param in
     * @param aggregationFunction
     * @return
     */
    private static List<Tuple2<String, List<Double>>> reduceDimensionality(List<Tuple2<String, List<Double>>> in,
                                                                           AggregationFunction aggregationFunction) {
        // TODO Currently this splits off the last entry and aggregates the first section. Update to what we want
        if (in.size() < 2) {
            throw new IllegalArgumentException("In order to reduce dimensionality, at least 2 values should be " +
                    "present, but only " + in.size() + " present.");
        }
        List<Tuple2<String, List<Double>>> out = new ArrayList<>();
        List<Tuple2<String, List<Double>>> reducedFirstPart = aggregationFunction.aggregate(in);
        if (reducedFirstPart.size() != 1) {
            throw new IllegalArgumentException("Given aggregation function should aggregate to 1 value, " +
                    "but aggregated to " + reducedFirstPart.size());
        }
        out.addAll(reducedFirstPart);
        out.addAll(in.subList(in.size()-1, in.size()));
        return out;
    }

    /**
     * For an ID/index along a given dimension, generate all the bucket IDs that this index falls into, which is the
     * ID of the buckets of the index on the given dimension paired with all combinations on the other dimensions
     *
     * @param segmentId
     * @param onDimension
     * @param dimensions
     * @return
     */
    private static List<Integer> getBucketNumbersForIdAlongDimension(int segmentId, int onDimension, int dimensions, int numSegments) {
        List<Integer> bucketIds = new ArrayList<>();
        // TODO Optimize?
        for (int bucketId = 0; bucketId < Math.pow(numSegments, dimensions); bucketId++) {
            if ((bucketId / (int) Math.pow(numSegments, onDimension)) % numSegments == segmentId) {
                bucketIds.add(bucketId);
            }
        }
        return bucketIds;
    }

    private JavaPairRDD<List<String>, Double> filterHighCorrelations(JavaPairRDD<List<String>, Double> correlations) {
        // TODO Filter out the combinations that have a high correlation only

        return correlations.filter(correlation -> {
            double value = correlation._2;
            double threshold = -Double.MAX_VALUE; // TODO refine
            return value > threshold;
        });
    }

    // simple plot function
    private void plot(List<Tuple2<String, List<Double>>> data) {
        // Create Dataset to Plot
        XYSeriesCollection dataset = new XYSeriesCollection();
        double idx = 0.0;
        for (Tuple2<String, List<Double>> stock : data) {
            XYSeries series = new XYSeries(stock._1);
            for (Double value : stock._2) {
                series.add(idx, value);
            }
            dataset.addSeries(series);
        }

        // Create UI
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Data Engineering",
                "day number",
                "value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
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

    /**
     * Generate a set of dates between a start and end date: one date during daytime for every workday
     * @param start
     * @param end
     * @return
     */
    static List<Date> generateDates(Date start, Date end) {
        LinkedList<Date> dates = new LinkedList<>();
        Date cur = start;
        while (cur.before(end)) {
            int weekday = cur.getDay();
            if (weekday != 0 && weekday != 6) dates.add(cur);
            cur = new Date(cur.getTime() + 86400000L);
        }
        dates.add(end);

        // Sorting for safety purposes
        dates.sort(Date::compareTo);

        return dates;
    }

    public static void main(String[] args) throws ParseException {

        // Load the config
        Properties config = new Properties();
        try {
            config.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            System.out.println("Please copy config.properties.example to config.properties and fill it in");
            e.printStackTrace();
            System.exit(-1);
        }

        // Show read config variables to user
        System.out.println("Reading data from " + config.getProperty("data_path"));
        System.out.println("Using Hadoop directory " + config.getProperty("hadoop_path"));
        System.out.println("Saving output to " + config.getProperty("output_path"));
        System.out.println("Setting master node to " + config.getProperty("master_node"));
        System.out.println("Using Spark driver bind address: " + config.getProperty("spark_driver"));
        System.out.println("Minimum partitions when reading files: " + config.getProperty("min_partitions"));
        System.out.println("Using " + config.getProperty("num_segments") + " segments to divide the stocks into");
        System.out.println("Running on server: " + config.getProperty("server"));
        System.out.println("Matching with stocks " + config.getProperty("data_match"));
        System.out.println("Using start date: " + config.getProperty("start_date"));
        System.out.println("Using end date: " + config.getProperty("end_date"));
        System.out.println("Comparing on #dimensions: " + config.getProperty("dimensions"));
        System.out.println("Excluding files with keywords: " + config.getProperty("exclusions"));

        // Parse the config
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        System.setProperty("hadoop.home.dir", config.getProperty("hadoop_path"));
        String path = config.getProperty("data_path");
        String outputPath = config.getProperty("output_path");
        String masterNode = config.getProperty("master_node");
        String sparkDriver = config.getProperty("spark_driver");
        int minPartitions = Integer.parseInt(config.getProperty("min_partitions"));
        int numSegments = Integer.parseInt(config.getProperty("num_segments"));
        boolean server = Boolean.parseBoolean(config.getProperty("server"));
        String source = config.getProperty("data_match"); // only considers stocks that contain `source` as a sub-string
        Date start_date = format.parse(config.getProperty("start_date"));
        Date end_date = format.parse(config.getProperty("end_date"));
        int dimensions = Integer.parseInt(config.getProperty("dimensions"));
        String[] exclusions = config.getProperty("exclusions").split(",");

        // Run the logic
        new Main(path, outputPath, source, start_date, end_date, masterNode, sparkDriver, minPartitions, numSegments,
                dimensions, server, exclusions);
    }
}