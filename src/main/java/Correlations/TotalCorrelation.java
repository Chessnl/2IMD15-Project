package Correlations;

import Aggregations.NormalizationAggregation;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

public class TotalCorrelation implements CorrelationFunction {

    private final int NUM_BUCKETS = 20;


    public double getCorrelation(List<Tuple2<String, List<Double>>> stocks) {
        if (stocks.size() < 2) throw new IllegalArgumentException("TotalCorrelation requires at least 2 vectors");
        stocks = new NormalizationAggregation().aggregate(stocks);

        int NUM_STOCKS = stocks.size();
        int NUM_ENTRIES = stocks.get(0)._2.size();

        // for each stock stores amount of prices [0,1/n), [1/n, 2/n), ... , [n-2/n, n-1/n), [n-1/n, 1] with n = NUM_BUCKETS
        int[][] indep_bucket = new int[NUM_STOCKS][NUM_BUCKETS];
        for (int stock_id = 0; stock_id < NUM_STOCKS; stock_id++) for (double price : stocks.get(stock_id)._2)
            indep_bucket[stock_id][price_to_bucket(price)]++;

        // stores for each bucket combination the amount of times it occured:
        // uses as key  x_1 * n^(m-1) + x_2 * n^(m-2) + ... + x_m * n^0
        // where x_i is the bucket number stock i has for this combination and m is the amount of stocks
        HashMap<Integer, Integer> shared_bucket = new HashMap<>();
        for (int entry = 0; entry < NUM_ENTRIES; entry++) {
            int key = 0;
            for (Tuple2<String, List<Double>> stock : stocks) {
                key *= this.NUM_BUCKETS;
                key += price_to_bucket(stock._2.get(entry));
            }
            if (!shared_bucket.containsKey(key)) shared_bucket.put(key, 0);
            shared_bucket.put(key, shared_bucket.get(key) + 1);
        }

        // calculates correlation, only iterates for cases in which p(x_1, x_2, ... , x_m) > 0
        double correlation = 0;
        for (int key : shared_bucket.keySet()) {
            double p_shared = shared_bucket.get(key) / (double) NUM_ENTRIES; // p_shared = p(x_1, x_2, ... , x_m)
            double p_indep = 1; // p_indep = p(x_1) * p(x+2) * ... * p(x_m)
            for (int stock_id = NUM_STOCKS - 1; stock_id >= 0; stock_id--) {
                p_indep *= indep_bucket[stock_id][key % this.NUM_BUCKETS] / (double) NUM_ENTRIES;
                key /= this.NUM_BUCKETS;
            }
            correlation += p_shared * Math.log(p_shared / p_indep);
        }

        return correlation;
    }

    private int price_to_bucket(double price) {
        // price in double[0, 1]
        // price * NUM_BUCKETS in double[0, NUM_BUCKETS]
        // floor (price * NUM_BUCKETS) in int[0, NUM_BUCKETS], this is 20 iff price = 1.0, assigning this price to bucket 19
        return (int) Math.floor(Math.min(this.NUM_BUCKETS - 1, price * (this.NUM_BUCKETS)));
    }
}
