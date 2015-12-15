//package org.janelia.thickness;
//
//import ij.process.FloatProcessor;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.broadcast.Broadcast;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//
///**
// * Created by hanslovskyp on 9/18/15.
// */
//public class MatrixCalculatorBenchmark {
//
//    public static JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>> runJoin(JavaPairRDD<Integer, ScalaFPTuple> rdd, JavaSparkContext sc, int range, int size)
//    {
//        HashMap< Integer, ArrayList< Integer > > map = new HashMap< Integer, ArrayList< Integer > >();
//        for( int i = 0; i < size; ++i )
//        {
//            ArrayList<Integer> al = new ArrayList<Integer>();
//            for ( int j = i + 1; j <= i + range && j < size; ++j )
//            {
//                al.add( j );
//            }
//            map.put( i, al );
//        }
//
//        final Broadcast<HashMap<Integer, ArrayList<Integer>>> bc = sc.broadcast(map);
//
//        JavaPairRDD<Tuple2<Integer, ScalaFPTuple>, ArrayList<Integer>> keysImagesListOfKeys = rdd
//                .mapToPair(new PairFunction<Tuple2<Integer, ScalaFPTuple>, Tuple2<Integer, ScalaFPTuple>, ArrayList<Integer>>() {
//                    public Tuple2<Tuple2<Integer, ScalaFPTuple>, ArrayList<Integer>> call(Tuple2<Integer, ScalaFPTuple> t) throws Exception {
//                        return Utility.tuple2(t, bc.getValue().get(t._1()));
//                    }
//                })
//                ;
//
//        JavaPairRDD<Integer, Tuple2<Integer, ScalaFPTuple>> flat = keysImagesListOfKeys
//                .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, ScalaFPTuple>, ArrayList<Integer>>, Tuple2<Integer, ScalaFPTuple>, Integer>() {
//                    public Iterable<Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer>> call(final Tuple2<Tuple2<Integer, ScalaFPTuple>, ArrayList<Integer>> t) throws Exception {
//                        return new Iterable<Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer>>() {
//                            public Iterator<Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer>> iterator() {
//                                return new Iterator<Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer>>() {
//                                    private final Iterator<Integer> it = t._2().iterator();
//
//                                    public boolean hasNext() {
//                                        return it.hasNext();
//                                    }
//
//                                    public Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer> next() {
//                                        return Utility.tuple2(t._1(), it.next());
//                                    }
//                                };
//                            }
//                        };
//                    }
//                })
//                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer>, Integer, Tuple2<Integer, ScalaFPTuple>>() {
//                    public Tuple2<Integer, Tuple2<Integer, ScalaFPTuple>> call(Tuple2<Tuple2<Integer, ScalaFPTuple>, Integer> t) throws Exception {
//                        return Utility.tuple2(t._2(), t._1());
//                    }
//                })
//                ;
//
//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>> joint = flat
//                .join(rdd)
//                .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple2<Integer, ScalaFPTuple>, ScalaFPTuple>>, Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>>() {
//                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>> call(Tuple2<Integer, Tuple2<Tuple2<Integer, ScalaFPTuple>, ScalaFPTuple>> t) throws Exception {
//                        Tuple2<Tuple2<Integer, ScalaFPTuple>, ScalaFPTuple> t2 = t._2();
//                        Tuple2<Integer, ScalaFPTuple> t21 = t2._1();
//                        return Utility.tuple2(Utility.tuple2(t._1(), t21._1()), Utility.tuple2(t21._2(), t2._2()));
//                    }
//                });;
//
//        return joint;
//
//    }
//
//    public static JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>> runCartesian(JavaPairRDD<Integer, ScalaFPTuple> rdd, final int range)
//    {
//
//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>> filteredCartesion = rdd
//                .cartesian(rdd)
//                .filter(new Function<Tuple2<Tuple2<Integer, ScalaFPTuple>, Tuple2<Integer, ScalaFPTuple>>, Boolean>() {
//                    public Boolean call(Tuple2<Tuple2<Integer, ScalaFPTuple>, Tuple2<Integer, ScalaFPTuple>> t) throws Exception {
//                        int diff = t._1()._1().intValue() - t._2()._1().intValue();
//                        return diff > 0 && diff <= range;
//                    }
//                })
//                .mapToPair(new PairFunction<Tuple2<Tuple2<Integer, ScalaFPTuple>, Tuple2<Integer, ScalaFPTuple>>, Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>>() {
//                    public Tuple2<Tuple2<Integer, Integer>, Tuple2<ScalaFPTuple, ScalaFPTuple>> call(Tuple2<Tuple2<Integer, ScalaFPTuple>, Tuple2<Integer, ScalaFPTuple>> t) throws Exception {
//                        Tuple2<Integer, ScalaFPTuple> t1 = t._1();
//                        Tuple2<Integer, ScalaFPTuple> t2 = t._2();
//                        return Utility.tuple2(Utility.tuple2(t1._1(), t2._1()), Utility.tuple2(t1._2(), t2._2()));
//                    }
//                });;
//        return filteredCartesion;
//
//    }
//
//    public static void main( String[] args )
//    {
//        final int width = Integer.parseInt( args[ 0 ] );
//        final int height = Integer.parseInt( args[ 1 ] );
//        final int size = Integer.parseInt( args[ 2 ] );
//        final int range = Integer.parseInt(args[3]);
//        SparkConf conf = new SparkConf()
//                .setAppName( "JoinAndCartesianBenchmark" )
////                .registerKryoClasses( new Class[] { ScalaFPTuple.class } )
//                ;
//        final JavaSparkContext sc = new JavaSparkContext( conf );
//
//        ArrayList< Integer > indices = Utility.arange(size);
//
//        JavaPairRDD<Integer, ScalaFPTuple> indexedImages = sc
//                .parallelize( indices )
//                .mapToPair( new PairFunction< Integer, Integer, ScalaFPTuple>() {
//
//                    public Tuple2<Integer, ScalaFPTuple> call( Integer arg0 ) throws Exception {
//                        FloatProcessor fp = new FloatProcessor( width,  height );
//                        return Utility.tuple2( arg0, ScalaFPTuple.create(fp) );
//                    }
//
//                })
//                .sortByKey()
//                .cache()
//                ;
//
//        indexedImages.count();
//
//        String rangeString = "range = " + String.format( "%3d", range );
//
//        long t0_join_sorted = System.currentTimeMillis();
//        long countJoinSorted = runJoin( indexedImages, sc, range, size ).count();// jt.cartesianFilter( indexedImages, filter ).count();
//        long t1_join_sorted = System.currentTimeMillis();
//        String joinSortedString = String.format("%25dms", (t1_join_sorted - t0_join_sorted));
//
//
//        long t0_cartesian = System.currentTimeMillis();
//        long countCartesian = runCartesian( indexedImages, range ).count();
//        long t1_cartesian = System.currentTimeMillis();
//
//        String joinString = String.format( "%25dms", ( t1_cartesian - t0_cartesian ) );
//
//        if ( countJoinSorted != countCartesian )
//            System.out.println( countJoinSorted + "!=" + countCartesian + " for range=" + range );
//
//        System.out.println(
//                String.format(
//                        "start=%d stop=%d range=%d width=%d height=%d runtime-join=%dms runtime-cartesion=%dms",
//                        0, size, range, width, height, t1_join_sorted - t0_join_sorted, t1_cartesian - t0_cartesian )
//        );
////        System.out.println( String.format( "%-20s", rangeString ) + joinSortedString + " (join)" + joinString + " (cartesian)" );
//
//        sc.close();
//
//    }
//
//}
