package test;

import static org.junit.jupiter.api.Assertions.*;

import Correlations.CorrelationFunction;
import Correlations.PearsonCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

class PearsonCorrelationTest {

    private final CorrelationFunction correl = new PearsonCorrelation();

    @Test
    void test1() {
        List<Double> first = new LinkedList<>();
        List<Double> second = new LinkedList<>();

        for (double i = 0; i < 10; i++){
            first.add(i);
            second.add(i);
        }
        List<Tuple2<String, List<Double>>> stocks = Arrays.asList(
                new Tuple2<String, List<Double>>("1", first),
                new Tuple2<String, List<Double>>("2", second)
        );
        assertEquals(correl.getCorrelation(stocks), 1);
    }

    @Test
    void test2(){
        List<Double> first = new LinkedList<Double>(){
            {
                add(10.0);
                add(4.0);
                add(7.0);
                add(2.0);
                add(6.0);
                add(8.0);
            }
        };

        List<Double> second = new LinkedList<Double>(){
            {
                add(1.0);
                add(3.0);
                add(7.0);
                add(4.0);
                add(9.0);
                add(10.0);
            }
        };
        List<Tuple2<String, List<Double>>> stocks = Arrays.asList(
                new Tuple2<String, List<Double>>("1", first),
                new Tuple2<String, List<Double>>("2", second)
        );
        double error = Math.abs(correl.getCorrelation(stocks)-0.0655);
        assertTrue(error < 0.01);
    }


}