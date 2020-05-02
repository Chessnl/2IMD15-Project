package Correlations;

import javafx.util.Pair;
import scala.Serializable;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class MutualInformationCorrelation implements CorrelationFunction, Serializable {

    @Override
    public double getCorrelation(List<Tuple2<Date, Double>> first, List<Tuple2<Date, Double>> second) {
        int nrBuckets = 20;

        // Determine min and max
        Pair<Double, Double> minMaxX = findMinMax(first);
        Pair<Double, Double> minMaxY = findMinMax(second);

        // Prepare equi-width histograms
        double[] histX = new double[nrBuckets];
        double[] histY = new double[nrBuckets];
        double[][] histXY = new double[nrBuckets][nrBuckets];

        // Determine steps
        double stepX = minMaxX.getKey() / minMaxX.getValue();
        double stepY = minMaxY.getKey() / minMaxY.getValue();
        double singleIncr = 1.0/first.size();
        double doubleIncr = 1.0/(first.size()*second.size());

        // Prepare for simultaneous looping
        Iterator<Tuple2<Date, Double>> iterX = first.iterator();
        Iterator<Tuple2<Date, Double>> iterY = second.iterator();

        if(!first.isEmpty() && !second.isEmpty()) {

            // Loop over both timeseries
            while (iterX.hasNext() && iterY.hasNext()){
                double xi = iterX.next()._2;
                double yi = iterY.next()._2;

                // Determine both buckets
                int bucketX = (int) ((xi-minMaxX.getKey()) / stepX);
                int bucketY = (int) ((yi-minMaxY.getKey()) / stepY);

                histX[bucketX] += singleIncr;
                histY[bucketY] += singleIncr;
                histXY[bucketX][bucketY] += doubleIncr;
            }
        }

        // Calculate for all histogram bucket combinations
        double MI = 0;
        for (int i = 0; i < nrBuckets; i++){
            for (int j = 0; j < nrBuckets; j++){
                MI += histXY[i][j] * Math.log( (histXY[i][j]) / (histX[i]*histY[j]) );
            }
        }

        return MI;
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
