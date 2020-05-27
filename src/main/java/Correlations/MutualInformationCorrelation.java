package Correlations;

import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public class MutualInformationCorrelation extends CorrelationFunction {

    public MutualInformationCorrelation(double treshold) {
        super(treshold);
    }

    @Override
    public double getCorrelation(List<Tuple2<String, List<Double>>> stocks) {
        if (stocks.size() != 2) throw new IllegalArgumentException(
                "MutualInformationCorrelation requires exactly 2 vectors, got " + stocks.size());
        int nrBuckets = 20;
        List<Double> first = stocks.get(0)._2;
        List<Double> second = stocks.get(1)._2;

        // Determine min and max
        Tuple2<Double, Double> minMaxX = findMinMax(first);
        Tuple2<Double, Double> minMaxY = findMinMax(second);

        // Prepare equi-width histograms
        double[] histX = new double[nrBuckets];
        double[] histY = new double[nrBuckets];
        double[][] histXY = new double[nrBuckets][nrBuckets];

        // Determine steps
        double stepX = (minMaxX._2-minMaxX._1)/(nrBuckets-1);
        double stepY = (minMaxY._2-minMaxY._1)/(nrBuckets-1);
        double increment = 1.0/first.size();

        // Prepare for simultaneous looping
        Iterator<Double> iterX = first.iterator();
        Iterator<Double> iterY = second.iterator();

        if(!first.isEmpty() && !second.isEmpty()) {

            // Loop over both timeseries
            while (iterX.hasNext() && iterY.hasNext()){
                double xi = iterX.next();
                double yi = iterY.next();

                // Determine both buckets
                int bucketX = (int) ((xi-minMaxX._1) / stepX);
                int bucketY = (int) ((yi-minMaxY._1) / stepY);

                histX[bucketX] += increment;
                histY[bucketY] += increment;
                histXY[bucketX][bucketY] += increment;
            }
        }

        // Calculate for all histogram bucket combinations
        double MI = 0;
        for (int i = 0; i < nrBuckets; i++){
            for (int j = 0; j < nrBuckets; j++){
                double result = histXY[i][j] * Math.log( (histXY[i][j]) / (histX[i]*histY[j]) );
                if (!Double.isNaN(result)) {
                    MI += result;
                }
            }
        }

        return MI;
    }

    // Find the minimum and maximum of a single dataset
    private Tuple2<Double, Double> findMinMax(List<Double> data){
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        // Loop over the set to discover the min and max values
        for (Double point : data){
            if (point < min){
                min = point;
            }
            if (point > max){
                max = point;
            }
        }
        return new Tuple2<>(min, max);
    }
}
