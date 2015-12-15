package org.janelia.thickness;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.Views;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hanslovskyp on 9/24/15.
 */
public interface MatrixGeneration< K > {

    public JavaPairRDD< K, FPTuple > generateMatrices( final int[] stride, final int[] correlationBlockRadius, final int range );


//    public static JavaPairRDD<Tuple2< Long, Long >, FPTuple > generateMatricesFromManySmallBlocks(
//            JavaSparkContext sc,
//            JavaPairRDD< Long, FPTuple > sections,
//            final long blockRadius[],
//            final int range,
//            final long size,
//            final Broadcast<ArrayList< Tuple2< Long, Long > >> coordinates
//    )
//    {
//        JavaPairRDD<Tuple2<Long, Long>, ArrayList<FPTuple>> columns = sections // sections: index -> single image
//                .mapToPair( new Divider( coordinates, blockRadius ) ) // index -> { (x,y) : image }
//                .flatMapToPair( new RevertIndex() ) // (x, y) ->  (index, image) // (not unique)
//                .reduceByKey( new Utility.MergeMap< Long, FPTuple, HashMap< Long, FPTuple >>() ) // (x, y) -> { index: image }
//                .mapToPair( new Utility.DenseMapToArrayList< Tuple2< Long, Long >, FPTuple, HashMap<Long, FPTuple>>() ) // (x, y) -> image[]
//                .cache()
//                ;
//
//
//        JavaPairRDD<Tuple2<Long, Long>, FPTuple> matrices = columns
//                .mapToPair( new MatrixCalculator< Tuple2< Long, Long > >( range ) ) // (x, y) -> matrix
//                ;
//        return matrices;
//    }
//
//
//    public static JavaPairRDD< Tuple2< Long, Long >, FPTuple > generateMatricesFromFewLargeBlocks(
//            final JavaSparkContext sc,
//            final JavaPairRDD< Long, FPTuple > sections,
//            final long[] blockRadius,
//            final int range,
//            final long size,
//            final Broadcast< ArrayList< Tuple2< Long, Long > > > coordinates
//    )
//    {
//
//        JoinTest<Long, FPTuple> jt = new JoinTest< Long, FPTuple >();
//
//        ArrayList<Tuple2<Tuple2<Long, Long>, FPTuple>> result = new ArrayList< Tuple2< Tuple2< Long, Long >, FPTuple > >();
//        for ( final Tuple2<Long, Long> c : coordinates.getValue() )
//        {
//            JavaPairRDD<Long, FPTuple> subsections = sections // index -> image
//                    .mapToPair( new Subsection< Long >( c, blockRadius) ) // index -> subset of image
//                    ;
//
//            HashMap<Long, ArrayList<Long>> keySelections = new HashMap< Long, ArrayList< Long > >();
//
//            for ( long i = 0; i < size; ++i )
//            {
//                ArrayList<Long> currentSelections = new ArrayList< Long >();
//                for ( long k = i+1; k - i <= range && k < size; ++k )
//                {
//                    currentSelections.add( k );
//                }
//                keySelections.put( i, currentSelections );
//            }
//
//            JavaPairRDD<Tuple2<Long, Long>, Double> similarities =
//                    jt
//                            .projectOntoSelf( subsections, sc.broadcast( keySelections ) )
//                            .mapToPair( new Similarity2<Long, Long>()) // (index, index) -> similarity
//                    ;
//
////			JavaPairRDD<Tuple2<Long, Long>, Double> similarities = subsections
////					.cartesian( subsections ) // all combinations of images
////					.filter( new Utility.SelectSymmetricallyWithinRange< Long, FPTuple >( range ) ) // ( (index, image), (index, image) )
////					.mapToPair( new Similarity<Long, Long>()) // (index, index) -> similarity
////					;
//
//            FloatProcessor fp = new FloatProcessor( (int)size, (int)size );
//            fp.add( Double.NaN );
//            for ( int i = 0; i < size; ++i )
//                fp.setf( i, i, 1.0f );
//            List<Tuple2<Tuple2<Long, Long>, Double>> collectedSimilarities = similarities.collect();
//            for ( Tuple2<Tuple2<Long, Long>, Double> s : collectedSimilarities )
//            {
//                final int x = s._1()._1().intValue();
//                final int y = s._1()._2().intValue();
//                final float val = s._2().floatValue();
//                fp.setf( x, y, val );
//                fp.setf( y, x, val );
//            }
//            result.add( Utility.tuple2( c, new FPTuple( fp ) ) );
//        }
//        return sc.parallelizePairs( result );
//    }
//
//
//    //
//    // Functions for mapping RDDS:
//    //
//
//
//    public static class Divider implements PairFunction<Tuple2<Long,FPTuple>, Long, HashMap< Tuple2< Long, Long >, FPTuple > >
//    {
//        private static final long serialVersionUID = 309509487201983820L;
//        private final Broadcast< ArrayList< Tuple2< Long, Long > > > coordinates;
//        private final long[] blockRadius;
//
//        public Divider(
//                Broadcast<ArrayList<Tuple2<Long, Long>>> coordinates,
//                long[] blockRadius) {
//            super();
//            this.coordinates = coordinates;
//            this.blockRadius = blockRadius;
//        }
//
//        public Tuple2<Long, HashMap<Tuple2<Long, Long>, FPTuple>> call(
//                Tuple2<Long, FPTuple> t) throws Exception {
//            ExtendedRandomAccessibleInterval<FloatType, Img<FloatType>> extended = Views.extendBorder(ImageJFunctions.wrapFloat(new ImagePlus("", t._2().rebuild())));
//            final int width = t._2().width;
//            final int height = t._2().height;
//            HashMap<Tuple2<Long, Long>, FPTuple> result = new HashMap< Tuple2< Long, Long >, FPTuple >();
//            for ( Tuple2<Long, Long> tuple : coordinates.getValue() )
//            {
//                long x = tuple._1().longValue();
//                long y = tuple._2().longValue();
//                long[] lower = new long[] { Math.max( 0, x - blockRadius[0] ), Math.max( 0, y - blockRadius[1] ) };
//                long[] upper = new long[] { Math.min( width - 1, x + blockRadius[0] ), Math.min( height - 1, y + blockRadius[1] ) };
//                int fpWidth = (int) ( upper[0] - lower[0] + 1);
//                int fpHeight = (int) ( upper[1] - lower[1] + 1 );
//                FloatProcessor fp = new FloatProcessor( fpWidth, fpHeight );
//                FinalInterval interval = new FinalInterval( lower, upper );
//
//                RandomAccess<FloatType> ra = Views.interval( extended, interval ).randomAccess();
//                for ( int c1 = 0; c1 < fpWidth; ++c1 )
//                {
//                    ra.setPosition( c1 + lower[0], 0 );
//                    for ( int c2 =0; c2 < fpHeight; ++c2 )
//                    {
//                        ra.setPosition( c2 + lower[1], 1 );
//                        fp.setf( c1,  c2, ra.get().get() );
//                    }
//                }
//                result.put( tuple, new FPTuple( fp ) );
//            }
//            return Utility.tuple2( t._1(), result );
//        }
//    }
//
//
//    public static class RevertIndex implements PairFlatMapFunction<Tuple2<Long,HashMap<Tuple2<Long,Long>,FPTuple>>, Tuple2< Long, Long >, HashMap< Long, FPTuple > >
//    {
//        private static final long serialVersionUID = -8521266581696226186L;
//
//        public Iterable<Tuple2<Tuple2<Long, Long>, HashMap<Long, FPTuple>>> call( Tuple2<Long, HashMap<Tuple2<Long, Long>, FPTuple>> t ) throws Exception
//        {
//            ArrayList<Tuple2<Tuple2<Long, Long>, HashMap<Long, FPTuple>>> result = new ArrayList<Tuple2<Tuple2<Long, Long>, HashMap<Long, FPTuple>>>();
//            Long constant = t._1();
//            for ( Map.Entry<Tuple2<Long, Long>, FPTuple> entry : t._2().entrySet() )
//            {
//                HashMap<Long, FPTuple> hm = new HashMap< Long, FPTuple >();
//                hm.put( constant, entry.getValue() );
//                result.add( Utility.tuple2( entry.getKey(), hm ) );
//            }
//            return result;
//        }
//    }
//
//
//    public static class MatrixCalculator< K > implements PairFunction< Tuple2<K,ArrayList<FPTuple> >, K, FPTuple >
//    {
//        private static final long serialVersionUID = 5896308677817861243L;
//        private final int range;
//
//        public MatrixCalculator(int range) {
//            super();
//            this.range = range;
//        }
//
//        public Tuple2<K, FPTuple> call(
//                Tuple2<K, ArrayList<FPTuple> > t) throws Exception {
//            ArrayList<FPTuple> fpTuples = t._2();
//            int length = fpTuples.size();
//            FloatProcessor result = new FloatProcessor( length, length );
//            result.add( Double.NaN );
//            for ( int i = 0; i < length; ++ i )
//            {
//                result.setf( i, i, 1.0f );
//                for ( int k = i + 1; k - i <= range && k < length; ++k )
//                {
//                    final float val = (float)Correlations.calculate( fpTuples.get( i ).rebuild(), fpTuples.get( k ).rebuild() );
//                    result.setf( i, k, val );
//                    result.setf( k, i, val );
//                }
//            }
//            return Utility.tuple2( t._1(), new FPTuple( result ) );
//        }
//    }
//
//
//    public static class Subsection< K > implements PairFunction<Tuple2<K,FPTuple>, K, FPTuple>
//    {
//        private static final long serialVersionUID = 886079932104665922L;
//        private final Tuple2< Long, Long > coordinate;
//        private final long[] blockRadius;
//
//        public Subsection(Tuple2<Long, Long> coordinate, long[] blockRadius) {
//            super();
//            this.coordinate = coordinate;
//            this.blockRadius = blockRadius;
//        }
//
//        public Tuple2<K, FPTuple> call(Tuple2<K, FPTuple> t)
//                throws Exception {
//            FPTuple fpTuple = t._2();
//            ExtendedRandomAccessibleInterval<FloatType, Img<FloatType>> extended = Views.extendBorder( ImageJFunctions.wrapFloat( new ImagePlus( "", fpTuple.rebuild() )));
//            final long x = coordinate._1().longValue();
//            final long y = coordinate._2().longValue();
//            long[] lower = new long[] { Math.max( 0, x - blockRadius[0] ), Math.max( 0, y - blockRadius[1] ) };
//            long[] upper = new long[] { Math.min( fpTuple.width - 1, x + blockRadius[0] ), Math.min( fpTuple.height - 1, y + blockRadius[1] ) };
//            int fpWidth = (int) ( upper[0] - lower[0] + 1);
//            int fpHeight = (int) ( upper[1] - lower[1] + 1 );
//            FloatProcessor fp = new FloatProcessor( fpWidth, fpHeight );
//            FinalInterval interval = new FinalInterval( lower, upper );
//            Cursor<FloatType> cursor = Views.flatIterable( Views.interval( extended, interval ) ).cursor();
//            while( cursor.hasNext() )
//            {
//                cursor.fwd();
//
//                fp.setf( cursor.getIntPosition( 0 ) - (int)lower[0], cursor.getIntPosition( 1 ) - (int)lower[1], cursor.get().get() );
//            }
//            return Utility.tuple2( t._1(), new FPTuple( fp ) );
//        }
//    }
//
//
//    public static class Similarity< K, L > implements PairFunction<Tuple2<Tuple2<K,FPTuple>,Tuple2<L,FPTuple>>, Tuple2< K, L >, Double >
//    {
//
//        private static final long serialVersionUID = -5410741707670643436L;
//
//        public Tuple2<Tuple2<K, L>, Double> call(
//                Tuple2<Tuple2<K, FPTuple>, Tuple2<L, FPTuple>> t)
//                throws Exception {
//            Tuple2<K, FPTuple> t1 = t._1();
//            Tuple2<L, FPTuple> t2 = t._2();
//            Double result = Correlations.calculate( t1._2().rebuild(), t2._2().rebuild() );
//            return Utility.tuple2( Utility.tuple2( t1._1(), t2._1() ), result );
//        }
//
//    }
//
//    public static class Similarity2< K, L > implements PairFunction<Tuple2<Tuple2<K,L>,Tuple2<FPTuple,FPTuple>>, Tuple2< K, L >, Double >
//    {
//
//        private static final long serialVersionUID = -7103338829517128044L;
//
//        public Tuple2<Tuple2<K, L>, Double> call(
//                Tuple2<Tuple2<K, L>, Tuple2<FPTuple, FPTuple>> t)
//                throws Exception {
//            Tuple2<K, L> t1 = t._1();
//            Tuple2<FPTuple, FPTuple> t2 = t._2();
//            Double result = Correlations.calculate( t2._1().rebuild(), t2._2().rebuild() );
//            return Utility.tuple2( Utility.tuple2( t1._1(), t1._2() ), result );
//        }
//
//    }

}
