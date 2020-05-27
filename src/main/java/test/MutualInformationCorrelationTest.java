package test;

import Correlations.CorrelationFunction;
import Correlations.MutualInformationCorrelation;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MutualInformationCorrelationTest {

    private final CorrelationFunction correl = new MutualInformationCorrelation(0.);

    @Test
    void test1(){
        List<Double> first = new LinkedList<Double>(){
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
        };

        List<Double> second = new LinkedList<Double>(){
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
        };

        List<Tuple2<String, List<Double>>> stocks = Arrays.asList(
                new Tuple2<String, List<Double>>("1", first),
                new Tuple2<String, List<Double>>("2", second)
        );
        double error = Math.abs(correl.getCorrelation(stocks) -0.050447408);

        System.out.print(correl.getCorrelation(stocks));
        assertTrue(error < 0.01);
    }


}