package Aggregations;

import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MinAggregation extends AggregationFunction {

    public MinAggregation() {
        super();
    }

    public MinAggregation(AggregationFunction prev) {
        super(prev);
    }

    @Override
    public List<Tuple2<String, List<Double>>> singleAggregation(List<Tuple2<String, List<Double>>> in) {
        String stockName = in.stream().map(stock -> stock._1).collect(Collectors.joining("+"));
        Double[] prices = new Double[in.get(0)._2.size()];
        Arrays.fill(prices, Double.MAX_VALUE);

        for (Tuple2<String, List<Double>> stock : in) {
            for (int i = 0; i < prices.length; i++) {
                prices[i] = Math.min(prices[i], stock._2.get(i));
            }
        }

        return Collections.singletonList(new Tuple2<>(stockName, Arrays.asList(prices)));
    }
}
