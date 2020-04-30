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

    private double[] createHist1D(List<Tuple2<Date, Double>> data, int nrBuckets){
        double[] hist = new double[nrBuckets];
        // TODO: Implement

        return hist;
    }

    private double[][] createHist2D(List<Tuple2<Date, Double>> data, int nrBuckets){
        double[][] hist = new double[nrBuckets][nrBuckets];
        // TODO: Implement

        return hist;
    }
}
