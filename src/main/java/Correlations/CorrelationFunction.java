package Correlations;

import scala.Serializable;
import scala.Tuple2;

import java.util.List;

public abstract class CorrelationFunction implements Serializable {

    private final double threshold;

    public CorrelationFunction(double treshold) {
        this.threshold = treshold;
    }

    public abstract double getCorrelation(List<Tuple2<String, List<Double>>> stocks);

    public double getThreshold(){ return threshold; }
}
