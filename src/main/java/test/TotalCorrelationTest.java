package test;

import Correlations.TotalCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
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

    @Test // test for associativity
    void test3() {
        List<Tuple2<String, List<Double>>> stocks = new ArrayList<>();

        stocks.add(new Tuple2<>("Stock1", new LinkedList<Double>(){{
            add(10.0);
            add(4.0);
            add(7.0);
            add(2.0);
            add(6.0);
            add(8.0);
        }}));

        stocks.add(new Tuple2<>("Stock2", new LinkedList<Double>(){{
            add(1.0);
            add(3.0);
            add(7.0);
            add(4.0);
            add(9.0);
            add(10.0);
        }}));

        stocks.add(new Tuple2<>("Stock3", new LinkedList<Double>(){{
            add(4.0);
            add(1.0);
            add(3.0);
            add(3.0);
            add(6.0);
            add(3.0);
        }}));

        double corr1 = correl.getCorrelation(stocks);
        Collections.shuffle(stocks);
        double corr2 = correl.getCorrelation(stocks);
        Collections.shuffle(stocks);
        double corr3 = correl.getCorrelation(stocks);

        assertTrue(Math.abs(corr1 - corr2) < 0.01 && Math.abs(corr2 - corr3) < 0.01);
    }
}