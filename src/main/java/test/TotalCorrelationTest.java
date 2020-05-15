package test;

import Correlations.CorrelationFunction;
import Correlations.MutualInformationCorrelation;
import Correlations.TotalCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TotalCorrelationTest {

    private final TotalCorrelation correl = new TotalCorrelation();

    @Test
    void test1(){
        List<Tuple2<String, List<Double>>> stocks = new ArrayList<>();

        stocks.add(new Tuple2<>("Stock1", new LinkedList<Double>(){{
            add(0.0);
            add(1.0);
        }}));

        stocks.add(new Tuple2<>("Stock2", new LinkedList<Double>(){{
            add(0.0);
            add(1.0);
        }}));

        double error = Math.abs(correl.getCorrelation(stocks)-Math.log(2));
        assertTrue(error < 0.01);
    }

    @Test
    void test2(){
        List<Tuple2<String, List<Double>>> stocks = new ArrayList<>();

        stocks.add(new Tuple2<>("Stock1", new LinkedList<Double>(){{
            add(0.0);
            add(1.0);
        }}));

        stocks.add(new Tuple2<>("Stock2", new LinkedList<Double>(){{
            add(0.0);
            add(1.0);
        }}));

        stocks.add(new Tuple2<>("Stock3", new LinkedList<Double>(){{
            add(0.0);
            add(1.0);
        }}));

        stocks.add(new Tuple2<>("Stock4", new LinkedList<Double>(){{
            add(0.0);
            add(1.0);
        }}));

        double error = Math.abs(correl.getCorrelation(stocks)-Math.log(8));
        assertTrue(error < 0.01);
    }
}