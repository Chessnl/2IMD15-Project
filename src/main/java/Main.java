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

    // Settings
    private static int numSegments;
    private static int dimensions;
    private static boolean server;

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
    private final AverageAggregation normalThenAverageAggregationFunction =
            new AverageAggregation(new NormalAverageAggregation());

    Main(String path, String outputPath, String source, Date start_date, Date end_date,
         String masterNode, String sparkDriver,
         int minPartitions, String[] exclusions) {

        // Define a new output folder based on date and time and copy the current config to it
        String outputFolder = "out_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

        // Copy config to output folder
        copyConfig(outputPath, outputFolder);

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

        // Compute the buckets
        JavaPairRDD<Integer, List<List<Tuple2<String, List<Double>>>>> buckets =
                computeBuckets(timeSeries);

        // Compute the PearsonCorrelation Function
        JavaPairRDD<List<String>, Double> pearsonCorrelations = calculateCorrelations(
                buckets,
                pearsonCorrelationFunction,
                normalThenAverageAggregationFunction,
                true
        );
        saveCorrelationResultsToFile(pearsonCorrelations, "Pearson", outputPath, outputFolder);

        // Compute the MutualInformation correlation
        JavaPairRDD<List<String>, Double> mutualCorrelations = calculateCorrelations(
                buckets,
                mutualInformationFunction,
                averageAggregationFunction,
                true
        );
        saveCorrelationResultsToFile(mutualCorrelations, "MutualInformation", outputPath, outputFolder);

        // Compute the TotalCorrelation
        JavaPairRDD<List<String>, Double> totalCorrelation = calculateCorrelations(
                buckets,
                totalCorrelationFunction,
                identityAggregationFunction,
                false
        );
        saveCorrelationResultsToFile(totalCorrelation, "TotalCorrelation", outputPath, outputFolder);

        sparkSession.stop();
    }

    /**
     * Copy the config.properties file to an output folder
     *
     * @param outputPath
     */
    private static void copyConfig(String outputPath, String outputFolder) {
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

    /**
     * Generate a set of dates between a start and end date: one date during daytime for every workday
     *
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

    /**
     * Parse the files of the stocks into a list of observations for each stock
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

                // Parse the timeseries out of the files
                .mapToPair(s -> {
                    // Obtain stockName from filename
                    String stockName = s._1
                            .replaceAll("file:/" + path, "")
                            .replaceAll("_NoExpiry.txt", "")
                            .split("_", 2)[1];

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

                    // Creates [(time, opening, highest, lowest, closing, volume)] for the input file
                    // Merges observations which share a timestamp
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
                })

                // Combine multiple files of observations for the same stock together
                .reduceByKey(ListUtils::union);
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
                // Only consider stocks which had at least 10 observations
                .filter(s -> s._2.size() >= 10)

                // Interpolate data
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
     * Get the segment number using a stock name
     */
    public static int getSegNumber(String stockName) {
        // Double mod as Java mods to (-numSegments, numSegments), but we want only [0, numSegments).
        return ((stockName.hashCode() % numSegments) + numSegments) % numSegments;
    }

    /**
     * Divide a list of stocks into buckets for comparison along a given number of dimensions using a given number
     * of segments
     *
     * @param timeSeries
     * @return
     */
    private JavaPairRDD<Integer, List<List<Tuple2<String, List<Double>>>>> computeBuckets(
            JavaPairRDD<String, List<Double>> timeSeries
    ) {
        // Give every stock a segment number
        JavaPairRDD<Integer, List<Tuple2<String, List<Double>>>> keyed = timeSeries.mapToPair(s -> {
            int hash = getSegNumber(s._1);
            List<Tuple2<String, List<Double>>> value = new ArrayList<>();
            value.add(new Tuple2<>(s._1, s._2));
            return new Tuple2<>(hash, value);
        });

        // Collect stocks of the same segment together
        JavaPairRDD<Integer, List<Tuple2<String, List<Double>>>> segments = keyed.reduceByKey(ListUtils::union);

        // Bucket the stocks along as many dimensions as wanted
        JavaPairRDD<Integer, // Bucket number
                List< // List of segments, one of each dimension (List.size = dimensions)
                        List<Tuple2< // List of stocks in this segment (List.size = numStocks / numSegments (on average))
                                String, // Stock name
                                List<Double> // Stock timeseries
                                >>>> buckets = segments.flatMapToPair(s -> {
            int segmentId = s._1; // The hash this segment is made of
            System.out.println("Segment " + segmentId + " contains " + s._2.size() + " stocks.");
            List<Tuple2<Integer, List<List<Tuple2<String, List<Double>>>>>> bucketAssignments = new ArrayList<>();
            for (int onDimension = 0; onDimension < dimensions; onDimension++) { // For every axis this stock is along
                // Add it to all values for the other dimensions
                List<Integer> bucketNumbers = new ArrayList<>();
                addBucketNumbersForIdAlongDimension(bucketNumbers, new ArrayList<>(), segmentId, onDimension);
                for (int bucketNumber : bucketNumbers) {
                    List<List<Tuple2<String, List<Double>>>> bucketAssignment = new ArrayList<>();
                    bucketAssignment.add(s._2);
                    bucketAssignments.add(new Tuple2<>(bucketNumber, bucketAssignment));
                }
            }
            return bucketAssignments.iterator();
        })

        // Collect comparison-tuples that are in the same bucket together
        .reduceByKey(ListUtils::union);

        // Only consider buckets with as many values as dimensions in case not every segment has at least one stock
        buckets = buckets.filter(bucket -> bucket._2.size() == dimensions);

        return buckets;
    }

    /**
     * For a segment ID/index along a given dimension, generate all the buckets IDs that this index falls into, which is
     * the ID of the buckets of the index of the given dimension paired with all combinations on the other dimensions,
     * but only once for different ordering of the same set of segments that can fall into a bucket. Add all these
     * bucketIds to the given buckets array.
     * @param buckets Array to add the bucket numbers to
     * @param segments Recursively built set of segments that will make up 1 bucket
     * @param segmentId ID of the segment we want to fix
     * @param onDimension Dimension this segment needs to be fixed at
     */
    private static void addBucketNumbersForIdAlongDimension(List<Integer> buckets, List<Integer> segments,
                                                            int segmentId, int onDimension) {
        if (segments.size() == dimensions) {
            // Segments is currently a valid combination of segments
            // Compute bucket number
            int bucketId = 0;
            int lastSeg = -1;
            for (int seg : segments) {
                if (seg < lastSeg) {
                    throw new IllegalStateException("Constructed segment list which is not in order: " + segments);
                }
                bucketId += seg;
                bucketId *= numSegments;
                lastSeg = seg;
            }

            // Add to the buckets list
            buckets.add(bucketId);
        } else {
            if (segments.size() == onDimension) {
                // We're at the dimension that this segment needs to be at, so only add that one
                List<Integer> segmentsCopy = new ArrayList<>(segments);
                segmentsCopy.add(segmentId);
                addBucketNumbersForIdAlongDimension(buckets, segmentsCopy, segmentId, onDimension);
            } else {
                // Add all segments as an option for this dimension, but only the ones that are at least as high
                // as the previous one (including equal) and ensure that if we are not yet at the dimension that needs
                // to be segment {segmentId}, ensure that it's lower than that (including equal)
                int firstSegment = segments.size() > 0 ? segments.get(segments.size()-1) : 0;
                int lastSegment = segments.size() < onDimension ? segmentId : numSegments - 1;
                for (int seg = firstSegment; seg <= lastSegment; seg++) {
                    List<Integer> segmentsCopy = new ArrayList<>(segments);
                    segmentsCopy.add(seg);
                    addBucketNumbersForIdAlongDimension(buckets, segmentsCopy, segmentId, onDimension);
                }
            }
        }
    }

    /**
     * Calculate all the correlations on a set of buckets
     *
     * @param buckets
     * @param correlationFunction
     * @param aggregationFunction
     * @param reduceDimensionality
     * @return
     */
    private JavaPairRDD<List<String>, Double> calculateCorrelations(
            JavaPairRDD<Integer, List<List<Tuple2<String, List<Double>>>>> buckets,
            CorrelationFunction correlationFunction,
            AggregationFunction aggregationFunction,
            boolean reduceDimensionality
    ) {
        return buckets.flatMapToPair(s ->
                compareAllPairs(
                        new ArrayList<>(),
                        s._2,
                        correlationFunction,
                        aggregationFunction,
                        reduceDimensionality
                ).iterator());
    }

    /**
     * Compute correlation of each combination of stocks from different dimensions recursively.
     *
     * @param compareThese Values that are already taken off of dimensions that are to be compared to the other dimensions
     * @param toAllCombinationsOfThese List of segments to compare to (the other dimensions)
     * @param correlationFunction
     * @param aggregationFunction
     * @param reduceDimensionality
     * @return List of all correlations on all combinations
     */
    private static List<Tuple2<List<String>, Double>> compareAllPairs(
            List<Tuple2<String, List<Double>>> compareThese,
            List<List<Tuple2<String, List<Double>>>> toAllCombinationsOfThese,
            CorrelationFunction correlationFunction,
            AggregationFunction aggregationFunction,
            boolean reduceDimensionality) {
        List<Tuple2<List<String>, Double>> out = new ArrayList<>();
        // NB: No double comparisons are done, nor against itself

        if (toAllCombinationsOfThese.isEmpty()) {
            // Done recursing, compute correlation

            List<String> stockNames = compareThese.stream().map(stock -> stock._1)
                    .collect(Collectors.toCollection(ArrayList::new));
            // Only compute correlation for tuples where all stocks are different (where no stock appears twice)
            if (stockNames.stream().distinct().count() == stockNames.size()) {
                // All stocks have a different name

                // TODO Here we need to handle taking/making different orderings of the given {compareThese} segments

                // Aggregate (with or without reducing dimensionality)
                List<Tuple2<String, List<Double>>> aggregated = reduceDimensionality ?
                        reduceDimensionality(compareThese, aggregationFunction) :
                        aggregationFunction.aggregate(compareThese);

                // Calculate correlation
                double correlation = correlationFunction.getCorrelation(aggregated);

                out.add(new Tuple2<>(stockNames, correlation));
            } else {
                throw new IllegalStateException("A stock (not segment) combination for correlation calculation " +
                        "contained duplicates: " + stockNames);
            }
        } else {
            // Recurse further by taking off another dimension (which is 1 segment)
            List<Tuple2<String, List<Double>>> segment = toAllCombinationsOfThese.get(0);
            toAllCombinationsOfThese.remove(segment);
            int segmentId = getSegNumber(segment.get(0)._1);
            List<String> namesInSameSegment = compareThese.stream()
                    .map(stock -> stock._1) // Get the names of the already selected stocks
                    .filter(name -> getSegNumber(name) == segmentId) // Only take the names that originate from same seg
                    .collect(Collectors.toList()); // Collect those names
            for (Tuple2<String, List<Double>> stock : segment) {
                // If the to be considered segment is the same segment as the last segment,
                // ensure its name is higher than the previous such that we only create one set per combination
                // regardless of order (so only 123, not also 132)
                if (namesInSameSegment.stream().allMatch(name -> name.compareTo(stock._1) < 0)) {
                    compareThese.add(stock);
                    out.addAll(compareAllPairs(compareThese, toAllCombinationsOfThese, correlationFunction,
                            aggregationFunction, reduceDimensionality));
                    compareThese.remove(stock);
                }
            }
            toAllCombinationsOfThese.add(segment);
        }
        return out;
    }

    /**
     * Reduce the dimensionality of the to be compared pairs to two dimensions using some aggregation function
     *
     * @param in
     * @param aggregationFunction
     * @return
     */
    private static List<Tuple2<String, List<Double>>> reduceDimensionality(List<Tuple2<String, List<Double>>> in,
                                                                           AggregationFunction aggregationFunction) {
        // Splits off the first entry and aggregates the last section.
        if (in.size() < 2) {
            throw new IllegalArgumentException("In order to reduce dimensionality, at least 2 values should be " +
                    "present, but only " + in.size() + " present.");
        }
        List<Tuple2<String, List<Double>>> out = new ArrayList<>();
        List<Tuple2<String, List<Double>>> reducedLastPart = aggregationFunction.aggregate(in.subList(1, in.size()));
        if (reducedLastPart.size() != 1) {
            throw new IllegalArgumentException("Given aggregation function should aggregate to 1 value, " +
                    "but aggregated to " + reducedLastPart.size());
        }
        out.addAll(reducedLastPart);
        out.addAll(in.subList(0, 1));
        return out;
    }

    /**
     * Save a set of correlation results results to a file
     *
     * @param result
     * @param name
     * @param outputPath
     * @param outputFolder
     */
    private void saveCorrelationResultsToFile(JavaPairRDD<List<String>, Double> result,
                                              String name, String outputPath, String outputFolder
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

    private JavaPairRDD<List<String>, Double> filterHighCorrelations(JavaPairRDD<List<String>, Double> correlations) {
        // TODO Filter out the combinations that have a high correlation only

        return correlations.filter(correlation -> {
            double value = correlation._2;
            double threshold = -Double.MAX_VALUE; // TODO refine
            return value > threshold;
        });
    }

    /**
     * Simple plot of some time-series
     *
     * @param data
     */
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
        numSegments = Integer.parseInt(config.getProperty("num_segments"));
        server = Boolean.parseBoolean(config.getProperty("server"));
        String source = config.getProperty("data_match"); // only considers stocks that contain `source` as a sub-string
        Date start_date = format.parse(config.getProperty("start_date"));
        Date end_date = format.parse(config.getProperty("end_date"));
        dimensions = Integer.parseInt(config.getProperty("dimensions"));
        String[] exclusions = config.getProperty("exclusions").split(",");

        // Run the logic
        new Main(path, outputPath, source, start_date, end_date, masterNode, sparkDriver, minPartitions, exclusions);
    }
}