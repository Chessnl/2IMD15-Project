package Aggregations;

import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MaxAggregation extends AggregationFunction {

    public MaxAggregation() {
        super();
    }

    public MaxAggregation(AggregationFunction prev) {
        super(prev);
    }

    @Override
    public List<Tuple2<String, List<Double>>> singleAggregation(List<Tuple2<String, List<Double>>> in) {
        String stockNames = in.stream().map(stock -> stock._1).collect(Collectors.joining(","));
        String stockName = "MAX(" + stockNames + ")";
        Double[] prices = new Double[in.get(0)._2.size()];
        Arrays.fill(prices, Double.MIN_VALUE);

        for (Tuple2<String, List<Double>> stock : in) {
            for (int i = 0; i < prices.length; i++) {
                prices[i] = Math.max(prices[i], stock._2.get(i));
            }
        }

        return Collections.singletonList(new Tuple2<>(stockName, Arrays.asList(prices)));
    }

}
