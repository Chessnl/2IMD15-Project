package Aggregations;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class NormalAverageAggregation extends AggregationFunction {


    /**
     * normalizes each stock price list independently by dividing by average.
     * this keeps percentual trends intact and has an average of 1.
     * @return
     */
    @Override
    public List<Tuple2<String, List<Double>>> singleAggregation(List<Tuple2<String, List<Double>>> in) {
        List<Tuple2<String, List<Double>>> aggregated = new ArrayList<>();

        for (Tuple2<String, List<Double>> stock : in) {
            double avgPrice = 0;
            for (int i = 0; i < stock._2.size(); i++) {
                avgPrice = stock._2.get(i);
            }
            avgPrice = avgPrice/ (double) stock._2.size();

            List<Double> prices = new ArrayList<>();
            for (Double price : stock._2) prices.add(price / avgPrice);
            aggregated.add(new Tuple2<>(stock._1, prices));
        }

        return aggregated;
    }
}
