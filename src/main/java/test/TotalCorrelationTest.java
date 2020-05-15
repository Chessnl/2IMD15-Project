package test;

import Correlations.CorrelationFunction;
import Correlations.TotalCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TotalCorrelationTest {

    @Test
    void test1(){
        List<Tuple2<String, List<Double>>> stocks = new ArrayList<>();

        stocks.add(new Tuple2<>("Stock1", new LinkedList<Double>(){{
                add(1.0);
                add(2.0);
                add(0.5);
                add(1.0);
                add(1.0);
                add(2.0);
        }}));

        stocks.add(new Tuple2<>("Stock2", new LinkedList<Double>(){{
                add(1.0);
                add(1.0);
                add(1.0);
                add(2.0);
                add(2.0);
                add(2.0);
        }}));

        System.out.print(new TotalCorrelation().getCorrelation(stocks));
    }

    @Test
    void test2(){
        List<Tuple2<String, List<Double>>> stocks = new ArrayList<>();

        stocks.add(new Tuple2<>("Stock1", new LinkedList<Double>(){
            {
                add(1.0);
                add(1.0);
                add(1.0);
                add(1.0);
                add(2.0);
                add(2.0);
                add(2.0);
                add(2.0);
                add(2.0);
            }
        }));

        stocks.add(new Tuple2<>("Stock1", new LinkedList<Double>(){
            {
                add(1.0);
                add(1.0);
                add(2.0);
                add(2.0);
                add(1.0);
                add(1.0);
                add(1.0);
                add(1.0);
                add(2.0);
            }
        }));

        double error = Math.abs(new TotalCorrelation().getCorrelation(stocks)-0.050447408);
        assertTrue(error < 0.01);
    }
}
