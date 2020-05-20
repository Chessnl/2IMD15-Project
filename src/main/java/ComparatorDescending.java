import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class ComparatorDescending implements Comparator<Tuple2<List<String>, Double>>, Serializable {
    public int compare(Tuple2<List<String>, Double> o1, Tuple2<List<String>, Double> o2) {
        return o2._2.compareTo(o1._2);
    }
}
