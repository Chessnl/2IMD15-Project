import scala.Tuple2;
import scala.math.Ordering;

import java.util.List;

public class OrderingAscending implements Ordering<Tuple2<List<String>, Double>> {
    @Override
    public int compare(Tuple2<List<java.lang.String>, java.lang.Double> x, Tuple2<List<java.lang.String>, java.lang.Double> y) {
        return y._2.compareTo(x._2);
    }

    @Override
    public boolean gt(Tuple2<List<java.lang.String>, java.lang.Double> x, Tuple2<List<java.lang.String>, java.lang.Double> y) {
        return y._2 > x._2;
    }
}
