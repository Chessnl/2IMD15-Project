package Correlations;

import scala.Tuple2;

import java.util.Date;
import java.util.List;

public class PearsonCorrelation implements CorrelationFunction {
    @Override
    public double getCorrelation(List<Tuple2<Date, Double>> first, List<Tuple2<Date, Double>> second) {
        // @TODO Implement



        return 0;
    }

    private double getAverage(List<Tuple2<Date, Double>> data){
        double sum = 0;
        if(!data.isEmpty()) {
            for (Tuple2<Date, Double> point: data) {
                sum += point._2;
            }
            return sum / data.size();
        }
        return 0;
    }


}
