package Aggregations;

import scala.Tuple2;

import java.util.List;

public class IdentityAggregation implements AggregationFunction {

    @Override
    public List<Tuple2<String, List<Double>>> aggregate(List<Tuple2<String, List<Double>>> in) {
        return in;
    }
}
