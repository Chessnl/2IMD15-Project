package Correlations;

import scala.Serializable;
import scala.Tuple2;

import java.util.Date;
import java.util.List;

public interface CorrelationFunction extends Serializable {
    double getCorrelation(List<Double> first, List<Double> second);
}
