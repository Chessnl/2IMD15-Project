package Aggregations;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class NormalizationAggregation implements AggregationFunction {


    /**
     * normalizes each stock price list independently to [0, 1] range
     * @return
     */
    @Override
    public List<Tuple2<String, List<Double>>> aggregate(List<Tuple2<String, List<Double>>> in) {
        List<Tuple2<String, List<Double>>> aggregated = new ArrayList<>();

        for (Tuple2<String, List<Double>> stock : in) {
            List<Double> prices = new ArrayList<>();
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;
            for (int i = 0; i < stock._2.size(); i++) {
                minPrice = Math.min(minPrice, stock._2.get(i));
                maxPrice = Math.max(maxPrice, stock._2.get(i));
            }

            for (Double price : stock._2) prices.add(Math.min(1, Math.max(0, price - minPrice) / (maxPrice - minPrice)));
            aggregated.add(new Tuple2<>(stock._1, prices));
        }

        return aggregated;
    }
}
