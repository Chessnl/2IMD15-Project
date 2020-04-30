package Correlations;

import scala.Tuple2;

import java.util.Date;
import java.util.List;

public class MutualInformationCorrelation implements CorrelationFunction {

    @Override
    public double getCorrelation(List<Tuple2<Date, Double>> first, List<Tuple2<Date, Double>> second) {
        // @TODO Implement
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

    private double[][] createHist2D(List<Tuple2<Date, Double>> data, int nrBuckets, double min, double max){
        double[][] hist = new double[nrBuckets][nrBuckets];
        // TODO: Implement

        return hist;
    }
}
