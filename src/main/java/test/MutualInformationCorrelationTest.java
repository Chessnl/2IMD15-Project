package test;

import Correlations.CorrelationFunction;
import Correlations.MutualInformationCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MutualInformationCorrelationTest {

    private final CorrelationFunction correl = new MutualInformationCorrelation();

    @Test
    void test1(){
        List<Tuple2<Date, Double>> first = new LinkedList<Tuple2<Date, Double>>(){
            {
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
            }
        };

        List<Tuple2<Date, Double>> second = new LinkedList<Tuple2<Date, Double>>(){
            {
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 1.0));
                add(new Tuple2<Date, Double>(new Date(), 2.0));
            }
        };

        double error = Math.abs(correl.getCorrelation(first, second)-0.050447408);

        System.out.print(correl.getCorrelation(first, second));
        assertTrue(error < 0.01);
    }


}