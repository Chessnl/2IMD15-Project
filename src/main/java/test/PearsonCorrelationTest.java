package test;

import static org.junit.jupiter.api.Assertions.*;

import Correlations.CorrelationFunction;
import Correlations.PearsonCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

class PearsonCorrelationTest {

    private final CorrelationFunction correl = new PearsonCorrelation();

    @Test
    void test1() {
        List<Tuple2<Date, Double>> first = new LinkedList<>();
        List<Tuple2<Date, Double>> second = new LinkedList<>();

        for (double i = 0; i < 10; i++){
            Date date = new Date();
            Tuple2<Date, Double> point = new Tuple2<Date, Double>(date, i);
            first.add(point);
            second.add(point);
        }

        assertEquals(correl.getCorrelation(first, second), 1);
    }

    @Test
    void test2(){
        List<Tuple2<Date, Double>> first = new LinkedList<Tuple2<Date, Double>>(){
            {
                add(new Tuple2<Date, Double>(new Date(), 10.0));
                add(new Tuple2<Date, Double>(new Date(), 4.0));
                add(new Tuple2<Date, Double>(new Date(), 7.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 6.0));
                add(new Tuple2<Date, Double>(new Date(), 8.0));
            }
        };

        List<Tuple2<Date, Double>> second = new LinkedList<Tuple2<Date, Double>>(){
            {
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 3.0));
                add(new Tuple2<Date, Double>(new Date(), 7.0));
                add(new Tuple2<Date, Double>(new Date(), 4.0));
                add(new Tuple2<Date, Double>(new Date(), 9.0));
                add(new Tuple2<Date, Double>(new Date(), 10.0));
            }
        };

        double error = Math.abs(correl.getCorrelation(first, second)-0.0655);
        assertTrue(error < 0.01);
    }


}