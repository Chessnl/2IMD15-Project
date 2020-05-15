package Correlations;

import Aggregations.AverageAggregation;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

public class TotalCorrelation {

    private final int NUM_BUCKETS = 20;

    public double getCorrelation(List<Tuple2<String, List<Double>>> stocks) {

        int num_stocks = stocks.size();

        if (num_stocks < 2) throw new IllegalArgumentException("TotalCorrelation requires at least 2 vectors");

        int num_entries = stocks.get(0)._2.size();

        stocks = new AverageAggregation().aggregate(stocks);

        int[][] indep_bucket = new int[num_stocks][NUM_BUCKETS];
        HashMap<Integer, Integer> shared_bucket = new HashMap<>();

        for (int stock_id = 0; stock_id < num_stocks; stock_id++) {
            for (double price : stocks.get(stock_id)._2) {
                indep_bucket[stock_id][(int) Math.floor(price * this.NUM_BUCKETS)]++;
            }
        }

        for (int entry = 0; entry < num_entries; entry++) {
            int key = 0;
            for (Tuple2<String, List<Double>> stock : stocks) {
                key *= this.NUM_BUCKETS;
                key += (int) Math.floor(stock._2.get(entry) * this.NUM_BUCKETS);
            }
            if (!shared_bucket.containsKey(key)) shared_bucket.put(key, 0);
            shared_bucket.put(key, shared_bucket.get(key) + 1);
        }

        double correlation = 0;
        for (int key : shared_bucket.keySet()) {
            double p_shared = shared_bucket.get(key) / num_stocks;
            double p_indep = 1;
            for (int entry = num_entries - 1; entry > 0; entry--) {
                p_indep *= indep_bucket[entry][key % this.NUM_BUCKETS];
                key /= this.NUM_BUCKETS;
            }
            correlation += p_shared * Math.log(p_shared / p_indep);
        }

        return correlation;
    }
}
