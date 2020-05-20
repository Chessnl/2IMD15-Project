package Aggregations;

import scala.Serializable;
import scala.Tuple2;
import scala.xml.Null;

import java.util.List;

public abstract class AggregationFunction implements Serializable {


    private final AggregationFunction previousOperation;

    public AggregationFunction() {
        this.previousOperation = null;
    }

    public AggregationFunction(AggregationFunction previousOperation) {
        this.previousOperation = previousOperation;
    }


    /**
     * given a list consisting of stocks with n values, return a (potentially singleton list) list of stocks with n values
     *
     * @param in a List<String stock-name, List<Double> prices>
     * @return a List<String stock-name, List<Double> prices>
     */
    public List<Tuple2<String, List<Double>>> aggregate(List<Tuple2<String, List<Double>>> in){
        if (previousOperation != null){
            in = previousOperation.aggregate(in);
        }
        return singleAggregation(in);
    }

    public abstract List<Tuple2<String, List<Double>>> singleAggregation(List<Tuple2<String, List<Double>>> in);

}

