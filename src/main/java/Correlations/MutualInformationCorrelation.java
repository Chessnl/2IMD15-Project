package Correlations;

import javafx.util.Pair;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import scala.Tuple2;

import java.util.Date;
import java.util.List;
import java.util.Collections;

public class MutualInformationCorrelation implements CorrelationFunction {

    @Override
    public double getCorrelation(List<Tuple2<Date, Double>> first, List<Tuple2<Date, Double>> second) {



        return 0;
    }



    private double[] createHist1D(List<Tuple2<Date, Double>> data, int nrBuckets, double min, double max){
        double step = (max-min)/nrBuckets;
        double[] hist = new double[nrBuckets];
        double increment = 1.0 / data.size();

        if(!data.isEmpty()) {
            for (Tuple2<Date, Double> point: data) {
                int bucket = (int) ((point._2-min) / step);
                hist[bucket] += increment;
            }
        }

        return hist;
    }

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

    private Pair<Double, Double> findGlobalMinMax(List<Tuple2<Date, Double>> dataX, List<Tuple2<Date, Double>> dataY){
        Pair<Double, Double> minMaxX = findMinMax(dataX);
        Pair<Double, Double> minMaxY = findMinMax(dataY);

        return new Pair<>(  Math.min(minMaxX.getKey(), minMaxY.getKey()),
                            Math.max(minMaxX.getValue(), minMaxY.getValue()));
    }

    private Pair<Double, Double> findMinMax(List<Tuple2<Date, Double>> data){
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;


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
