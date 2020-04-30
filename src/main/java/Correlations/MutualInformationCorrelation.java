package Correlations;

import javafx.util.Pair;
import scala.Serializable;
import scala.Tuple2;

import java.util.Date;
import java.util.List;

public class MutualInformationCorrelation implements CorrelationFunction, Serializable {

    @Override
    public double getCorrelation(List<Tuple2<Date, Double>> first, List<Tuple2<Date, Double>> second) {
        int nrBuckets = 20;

        // Prepare histograms
        Pair<Double, Double> minMax = findGlobalMinMax(first, second);
        double[] histX = createHist1D(first, nrBuckets, minMax.getKey(), minMax.getValue());
        double[] histY = createHist1D(second, nrBuckets, minMax.getKey(), minMax.getValue());
        double[][] histXY = createHist2D(first, second, nrBuckets, minMax.getKey(), minMax.getValue());

        // Calculate for all histogram bucket combinations
        double MI = 0;
        for (int i = 0; i < nrBuckets; i++){
            for (int j = 0; j < nrBuckets; j++){
                MI += histXY[i][j] * Math.log( (histXY[i][j]) / (histX[i]*histY[j]) );
            }
        }

        return MI;
    }

    // Create a 1D histogram to approximate a discrete probability distribution function
    private double[] createHist1D(List<Tuple2<Date, Double>> data, int nrBuckets, double min, double max){

        // Step info
        double step = (max-min)/nrBuckets;          // The size of one bucket
        double[] hist = new double[nrBuckets];      // The actual equi-width histogram
        double increment = 1.0 / data.size();       // 1/data.size() for each point sums to 1.

        if(!data.isEmpty()) {

            // Increment the correct buckets
            for (Tuple2<Date, Double> point: data) {
                int bucket = (int) ((point._2-min) / step);
                hist[bucket] += increment;
            }
        }

        return hist;
    }

    // Create a 2D histogram to approximate a discrete probability distribution function
    private double[][] createHist2D(List<Tuple2<Date, Double>> dataX, List<Tuple2<Date, Double>> dataY,
                                    int nrBuckets, double min, double max){

        double step = (max-min)/nrBuckets;
        double[][] hist = new double[nrBuckets][nrBuckets];
        double increment = 1.0 / (dataX.size()*dataY.size());   //X*Y sums add up to one, satisfying probality dist.

        if(!dataX.isEmpty() && !dataY.isEmpty()) {

            // Loop over both timeseries
            for (Tuple2<Date, Double> pointX: dataX) {
                for (Tuple2<Date, Double> pointY: dataY){

                    // Determine both buckets
                    int bucketX = (int) ((pointX._2-min) / step);
                    int bucketY = (int) ((pointY._2-min) / step);

                    hist[bucketX][bucketY] += increment;
                }
            }
        }

        return hist;
    }

    // Finds the ultimate minimum and maximum of two datasets
    private Pair<Double, Double> findGlobalMinMax(List<Tuple2<Date, Double>> dataX, List<Tuple2<Date, Double>> dataY){
        Pair<Double, Double> minMaxX = findMinMax(dataX);
        Pair<Double, Double> minMaxY = findMinMax(dataY);

        return new Pair<>(  Math.min(minMaxX.getKey(), minMaxY.getKey()),
                            Math.max(minMaxX.getValue(), minMaxY.getValue()));
    }

    // Find the minimum and maximum of a single dataset
    private Pair<Double, Double> findMinMax(List<Tuple2<Date, Double>> data){
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        // Loop over the set to discover the min and max values
        for (Tuple2<Date, Double> point : data){
            if (point._2 < min){
                min = point._2;
            }
            if (point._2 > max){
                max = point._2;
            }
        }
        return new Pair<>(min, max);
    }
}
