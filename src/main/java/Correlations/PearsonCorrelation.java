package Correlations;

import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.lang.Math;

public class PearsonCorrelation implements CorrelationFunction {
    @Override
    public double getCorrelation(List<Double> first, List<Double> second) {

        // Retrieve averages
        double avgX = getAverage(first);
        double avgY = getAverage(second);

        // Prepare for simultaneous looping
        double cov = 0;
        double stdX = 0;
        double stdY = 0;
        Iterator<Double> iterX = first.iterator();
        Iterator<Double> iterY = second.iterator();

        // Process for all elements in a simultaneous loop
        while (iterX.hasNext() && iterY.hasNext()){
            double xi = iterX.next();
            double yi = iterY.next();

            cov += (xi-avgX)*(yi-avgY);
            stdX += Math.pow((xi-avgX), 2);
            stdY += Math.pow((yi-avgY), 2);
        }

        // Solve final equation
        stdX = Math.sqrt(stdX);
        stdY = Math.sqrt(stdY);
        return cov/(stdX*stdY);
    }

    private double getAverage(List<Double> data){
        double sum = 0;
        if(!data.isEmpty()) {
            for (Double point: data) {
                sum += point;
            }
            return sum / data.size();
        }
        return 0;
    }


}
