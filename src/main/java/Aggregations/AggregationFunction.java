package Aggregations;

import scala.Tuple2;

import java.util.List;

public interface AggregationFunction {

    /**
     * given a list consisting of stocks with n values, return a (potentially singleton list) list of stocks with n values
     *
     * @param in a List<String stock-name, List<Double> prices>
     * @return a List<String stock-name, List<Double> prices>
     */
    List<Tuple2<String, List<Double>>> aggregate(List<Tuple2<String, List<Double>>> in);

}

