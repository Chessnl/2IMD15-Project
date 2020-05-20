package Aggregations;

import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MaxAggregation extends AggregationFunction {

    @Override
    public List<Tuple2<String, List<Double>>> singleAggregation(List<Tuple2<String, List<Double>>> in) {
        StringBuilder stockname = new StringBuilder();
        Double[] prices = new Double[in.get(0)._2.size()];
        Arrays.fill(prices, Double.MIN_VALUE);

        for (Tuple2<String, List<Double>> stock : in) {
            stockname.append("(").append(stock._1).append(") + ");
            for (int i = 0; i < prices.length; i++) {
                prices[i] = Math.max(prices[i], stock._2.get(i));
            }
        }

        return Collections.singletonList(new Tuple2<>(stockname.toString(), Arrays.asList(prices)));
    }

}
