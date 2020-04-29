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
}
