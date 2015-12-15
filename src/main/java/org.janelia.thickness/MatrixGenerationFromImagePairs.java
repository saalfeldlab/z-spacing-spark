package org.janelia.thickness;

import ij.process.FloatProcessor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by hanslovskyp on 9/25/15.
 */
public class MatrixGenerationFromImagePairs implements MatrixGeneration<Tuple2< Integer, Integer >> {

    private final JavaSparkContext sc;
    // assume only one of (i,j),(j,i) is present
    private final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< FPTuple, FPTuple > > sectionPairs;
    private final int[] dim;
    private final int size;

    public MatrixGenerationFromImagePairs(JavaSparkContext sc, JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> sectionPairs, int[] dim, int size) {
        this.sc = sc;
        this.sectionPairs = sectionPairs;
        this.dim = dim;
        this.size = size;
    }

    public void ensurePersistence()
    {
        sectionPairs.cache();
        sectionPairs.count();
    }


    @Override
    public JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> generateMatrices(int[] stride, int[] correlationBlockRadius, final int range) {

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2< FPTuple, FPTuple >> pairsWithinRange =
                sectionPairs.filter(new SelectInRange< Tuple2< FPTuple, FPTuple > >( range ) );
        pairsWithinRange.cache().count();
        System.out.println( "Filtered pairs." );

        CorrelationBlocks correlationBlocks = new CorrelationBlocks(correlationBlockRadius, stride);

        final Broadcast<ArrayList<CorrelationBlocks.Coordinate>> coordinates = sc.broadcast(correlationBlocks.generateFromBoundingBox(dim));

        JavaPairRDD<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> pairwiseCorrelations = pairsWithinRange
                .mapToPair(new SubSectionCorrelations( coordinates, dim ) );
        pairwiseCorrelations.cache().count();
        System.out.println( "Created subsections." );

        JavaPairRDD<Tuple2<Integer, Integer>, FPTuple> matrices = pairwiseCorrelations
                .flatMapToPair(new ExchangeIndexOrder() )
                .reduceByKey(new ReduceMaps())
                .mapToPair(new MapToFPTuple(size) );
        matrices.cache().count();
        System.out.println( "Calculated matrices." );

        return matrices;
    }

    public static class SelectInRange< V > implements Function<Tuple2<Tuple2<Integer, Integer>, V>, Boolean> {

        private final int range;

        public SelectInRange(int range) {
            this.range = range;
        }

        @Override
        public Boolean call(Tuple2<Tuple2<Integer, Integer>, V> t) throws Exception {
            Tuple2<Integer, Integer> indices = t._1();
            int diff = indices._1().intValue() - indices._2().intValue();
            return Math.abs(diff) <= range;
        }
    }

    public static class SubSectionCorrelations implements PairFunction<
            Tuple2<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>>,
            Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> {

        private final Broadcast<ArrayList<CorrelationBlocks.Coordinate>> coordinates;
        private final int[] dim;

        public SubSectionCorrelations(Broadcast<ArrayList<CorrelationBlocks.Coordinate>> coordinates, int[] dim) {
            this.coordinates = coordinates;
            this.dim = dim;
        }

        @Override
        public Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>
        call(Tuple2<Tuple2<Integer, Integer>, Tuple2<FPTuple, FPTuple>> t) throws Exception {
            FloatProcessor fp1 = t._2()._1().rebuild();
            FloatProcessor fp2 = t._2()._2().rebuild();
            int[] min = new int[]{0, 0};
            int[] currentStart = new int[2];
            int[] currentStop = new int[2];
            HashMap<Tuple2<Integer, Integer>, Double> result = new HashMap<Tuple2<Integer, Integer>, Double>();
            for (CorrelationBlocks.Coordinate coord : coordinates.getValue()) {
                Tuple2<Integer, Integer> local = coord.getLocalCoordinates();
                Tuple2<Integer, Integer> global = coord.getWorldCoordinates();
                Tuple2<Integer, Integer> radius = coord.getRadius();
                currentStart[0] = Math.max(min[0], global._1() - radius._1());
                currentStart[1] = Math.max(min[1], global._2() - radius._2());
                currentStop[0] = Math.min(dim[0], global._1() + radius._1());
                currentStop[1] = Math.min(dim[1], global._2() + radius._2());
                int[] targetDim = new int[]{currentStop[0] - currentStart[0], currentStop[1] - currentStart[1]};
                FloatProcessor target1 = new FloatProcessor(targetDim[0], targetDim[1]);
                FloatProcessor target2 = new FloatProcessor(targetDim[0], targetDim[1]);
                for (int ySource = currentStart[1], yTarget = 0; ySource < currentStop[1]; ++ySource, ++yTarget) {
                    for (int xSource = currentStart[0], xTarget = 0; xSource < currentStop[0]; ++xSource, ++xTarget) {
                        target1.setf(xTarget, yTarget, fp1.getf(xSource, ySource));
                        target2.setf(xTarget, yTarget, fp2.getf(xSource, ySource));
                    }
                }
                double correlation = Correlations.calculate(target1, target2);
                result.put(local, correlation);
            }
            return Utility.tuple2(t._1(), result);
        }
    }

    public static class ExchangeIndexOrder implements PairFlatMapFunction<
            Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>,
            Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> {
        @Override
        public Iterable<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>>
        call(Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> t) throws Exception {
            // z coordinate of sections
            final Tuple2<Integer, Integer> zz = t._1();
            final HashMap<Tuple2<Integer, Integer>, Double> corrs = t._2();

            return new Iterable<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>>() {
                @Override
                public Iterator<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>> iterator() {
                    return new Iterator<Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>>() {
                        Iterator<Map.Entry<Tuple2<Integer, Integer>, Double>> it = corrs.entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> next() {
                            Map.Entry<Tuple2<Integer, Integer>, Double> nextCorr = it.next();
                            Tuple2<Integer, Integer> xy = nextCorr.getKey();
                            HashMap<Tuple2<Integer, Integer>, Double> result = new HashMap<Tuple2<Integer, Integer>, Double>();
                            result.put(zz, nextCorr.getValue());
                            return Utility.tuple2(xy, result);
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
    }

    public static class ReduceMaps implements Function2<
            HashMap<Tuple2<Integer, Integer>, Double>, HashMap<Tuple2<Integer, Integer>, Double>,
            HashMap<Tuple2<Integer, Integer>, Double>> {
        @Override
        public HashMap<Tuple2<Integer, Integer>, Double> call(HashMap<Tuple2<Integer, Integer>, Double> hm1, HashMap<Tuple2<Integer, Integer>, Double> hm2) throws Exception {
            hm1.putAll(hm2);
            return hm1;
        }
    }

    public static class MapToFPTuple implements PairFunction<
            Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>>,
            Tuple2<Integer, Integer>, FPTuple> {

        private final int size;

        public MapToFPTuple(int size) {
            this.size = size;
        }

        @Override
        public Tuple2<Tuple2<Integer, Integer>, FPTuple> call(Tuple2<Tuple2<Integer, Integer>, HashMap<Tuple2<Integer, Integer>, Double>> t) throws Exception {
            FloatProcessor result = new FloatProcessor(size, size);
            result.add(Double.NaN);
            for (int z = 0; z < size; ++z)
                result.setf(z, z, 1.0f);
            for (Map.Entry<Tuple2<Integer, Integer>, Double> entry : t._2().entrySet()) {
                Tuple2<Integer, Integer> xy = entry.getKey();
                int x = xy._1();
                int y = xy._2();
                float val = entry.getValue().floatValue();
                result.setf(x, y, val);
                result.setf(y, x, val);
            }
            return Utility.tuple2(t._1(), new FPTuple(result));
        }
    }
}
