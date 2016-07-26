package org.janelia.thickness;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.SparkInterpolation.BiLinear;
import org.janelia.thickness.SparkInterpolation.NearestNeighbor;
import org.janelia.thickness.utility.Utility;
import org.junit.Test;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.interpolation.InterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class SparkInterpolationTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4555614181168686879L;
	
	public void testPolicy(
			JavaSparkContext sc,
			JavaPairRDD< Tuple2< Integer, Integer >, double[]> rdd,
			RandomAccessibleInterval< DoubleType > sourceImg,
			int[] dim,
			int[] sourceDim,
			int[] targetDim,
			int[] radii1,
			int[] steps1,
			int[] radii2,
			int[] steps2,
			SparkInterpolation.AssociationPolicy policy,
			InterpolatorFactory< RealComposite<DoubleType>, RandomAccessible< RealComposite< DoubleType > > > factory,
			double tolerance
			)
	{
		assertEquals( sourceDim[0]*sourceDim[1], rdd.count() );
		
        BlockCoordinates cbs1 = new BlockCoordinates(radii1, steps1);
        BlockCoordinates cbs2 = new BlockCoordinates(radii2, steps2);
                
        ArrayList<BlockCoordinates.Coordinate> newCoords = cbs2.generateFromBoundingBox(dim);
        ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>> mapping = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>();
        for( BlockCoordinates.Coordinate n : newCoords )
        {
            mapping.add( Utility.tuple2(
            				n.getLocalCoordinates(), 
            				cbs1.translateOtherLocalCoordiantesIntoLocalSpace(n)
            				) );
        }
        JavaPairRDD<Tuple2<Integer, Integer>, double[]> interpol = SparkInterpolation.interpolate(sc, rdd, sc.broadcast(mapping), sourceDim, policy);
        List<Tuple2<Tuple2<Integer, Integer>, double[]>> interpolCollected = interpol.collect();
        
        assertEquals( targetDim[0]*targetDim[1], interpolCollected.size() );

        

        ScaleAndTranslation tf = new ScaleAndTranslation(
                new double[]{steps1[0] * 1.0 / steps2[0], steps1[1] * 1.0 / steps2[1]},
                new double[]{(radii1[0] - radii2[0]) * 1.0 / steps2[0], (radii1[1] - radii2[1]) * 1.0 / steps2[1]}
        );

        CompositeIntervalView<DoubleType, RealComposite<DoubleType>> img = 
				Views.collapseReal( sourceImg );
        RealTransformRandomAccessible<RealComposite<DoubleType>, InverseRealTransform> transformed =
        		RealViews.transform(Views.interpolate(Views.extendBorder(img), factory), tf);

        RandomAccess< RealComposite<DoubleType>> t = transformed.randomAccess();
        for ( Tuple2<Tuple2<Integer, Integer>, double[]> bla : interpolCollected )
        {
        	final double[] array = bla._2();
            t.setPosition(new int[]{bla._1()._1(), bla._1()._2()});
        	RealComposite<DoubleType> comp = t.get();
        	for ( int z = 0; z < array.length; ++z )
        		assertEquals( comp.get( z ).get(), array[z], tolerance );
        }
		
	}

	@Test
	public void test() {
        SparkConf conf = new SparkConf()
        		.setAppName("InterpolationTest")
        		.setMaster("local[*]")
        		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        		.set("spark.kryo.registrator", KryoSerialization.Registrator.class.getName())
        		;
        Logger.getRootLogger().setLevel( Level.OFF );
        Logger.getLogger( "org" ).setLevel( Level.OFF );
        Logger.getLogger( "akka" ).setLevel( Level.OFF );
        Logger.getLogger( "spark" ).setLevel( Level.OFF );
        JavaSparkContext sc = new JavaSparkContext(conf);
        Logger.getRootLogger().setLevel( Level.OFF );
        Logger.getLogger( "org" ).setLevel( Level.OFF );
        Logger.getLogger( "akka" ).setLevel( Level.OFF );
        Logger.getLogger( "spark" ).setLevel( Level.OFF );
        sc.setLogLevel( "FATAL" );
        
        final int[] dim = new int[]{20, 20};
        final int[] radii1 = new int[]{5, 5};
        final int[] radii2 = new int[]{2, 2};
        final int[] steps1 = new int[]{5, 5};
        final int[] steps2 = new int[]{2, 2};
        final int[] sourceDim = new int[] { (dim[0]-radii1[0]) / steps1[0] , (dim[1]-radii1[1]) / steps1[1] };
        final int[] targetDim = new int[] { (dim[0]-radii2[0]) / steps2[0] , (dim[1]-radii2[1]) / steps2[1] };
        
        final int depth = 10;

        // set up source RDD
        BlockCoordinates cbs1 = new BlockCoordinates(radii1, steps1);
        ArrayList<BlockCoordinates.Coordinate> init = cbs1.generateFromBoundingBox(dim);
        @SuppressWarnings("serial")
		JavaPairRDD<Tuple2<Integer, Integer>, double[]> rdd = sc
                .parallelize(init)
                .mapToPair(new PairFunction<BlockCoordinates.Coordinate, Tuple2<Integer, Integer>, double[]>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, double[]> call(BlockCoordinates.Coordinate coordinate) throws Exception {
                        Tuple2<Integer, Integer> wc = coordinate.getWorldCoordinates();
                        double[] array = new double[depth];
                        for ( int z = 0; z < depth; ++z )
                        {
                        	array[z] = wc._1() + wc._2() * dim[0] + z;
                        }
                        return Utility.tuple2(coordinate.getLocalCoordinates(), array);
                    }
                })
                .cache()
                ;

        List<Tuple2<Tuple2<Integer, Integer>, double[]>> rddCollected = rdd.collect();

        // set up sourceImg
        ArrayImg<DoubleType, DoubleArray> sourceImg = ArrayImgs.doubles( sourceDim[0], sourceDim[1], depth );
        ArrayRandomAccess<DoubleType> ra = sourceImg.randomAccess();
        for( Tuple2<Tuple2<Integer, Integer>, double[]> rddC : rddCollected )
        {
        	final double[] array = rddC._2();
        	System.out.println( array.length + " " + depth );
        	for ( int z = 0; z < array.length; ++z )
        	{
        		System.out.println( rddC._1() + " " + Arrays.toString( sourceDim ) );
        		ra.setPosition( new int[] { rddC._1()._1(), rddC._1()._2(), z } );
        		ra.get().set( rddC._2()[z] );
        	}
        }
        
        NearestNeighbor nnPolicy = new SparkInterpolation.NearestNeighbor();
        NearestNeighborInterpolatorFactory<RealComposite<DoubleType>> nnFactory = new NearestNeighborInterpolatorFactory<>();
        testPolicy(sc, rdd, sourceImg, dim, sourceDim, targetDim, radii1, steps1, radii2, steps2, nnPolicy, nnFactory, 0.0);
        
        BiLinear linearPolicy = new SparkInterpolation.BiLinear();
        NLinearInterpolatorFactory<RealComposite<DoubleType>> linearFactory = new NLinearInterpolatorFactory<>();
        testPolicy(sc, rdd, sourceImg, dim, sourceDim, targetDim, radii1, steps1, radii2, steps2, linearPolicy, linearFactory, 1e-10);
        
        sc.close();
	}

}
