package Aggregations;

import scala.Tuple2;

import java.util.List;

public class IdentityAggregation extends AggregationFunction {

    @Override
    public List<Tuple2<String, List<Double>>> singleAggregation(List<Tuple2<String, List<Double>>> in) {
        return in;
    }
}
