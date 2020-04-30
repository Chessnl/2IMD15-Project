package Correlations;

import scala.Serializable;
import scala.Tuple2;

import java.util.Date;
import java.util.List;

public interface CorrelationFunction extends Serializable {
    double getCorrelation(List<Tuple2<Date, Double>> first, List<Tuple2<Date, Double>> second);
}
