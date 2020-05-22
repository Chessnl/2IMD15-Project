import scala.Tuple2;
import scala.math.Ordering;

import java.util.List;

public class OrderingDescending  implements Ordering<Tuple2<List<String>, Double>> {
    @Override
    public int compare(Tuple2<List<String>, Double> x, Tuple2<List<String>, Double> y) {
        return x._2.compareTo(y._2);
    }
}
