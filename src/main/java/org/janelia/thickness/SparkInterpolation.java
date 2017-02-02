package org.janelia.thickness;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.thickness.SparkInference.Variables;
import org.janelia.thickness.utility.Utility;

import net.imglib2.RandomAccess;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.realtransform.InverseRealTransform;
import net.imglib2.realtransform.RealTransformRandomAccessible;
import net.imglib2.realtransform.RealViews;
import net.imglib2.realtransform.ScaleAndTranslation;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;
import scala.Tuple2;

public class SparkInterpolation
{

	public static JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > interpolate(
			final JavaSparkContext sc,
			final JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > source,
			final Broadcast< ? extends List< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > > newCoordinatesToOldCoordinates, final int[] dim, final MatchCoordinates.Matcher matcher )
	{

		final JavaPairRDD< Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables >, Tuple2< Tuple2< Integer, Integer >, Double > > coordinateMatches =
				source.flatMapToPair( new MatchCoordinates( newCoordinatesToOldCoordinates, dim, matcher ) );
		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > > mapNewToOld =
				coordinateMatches.mapToPair( new SwapKey() );

		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > > weightedArrays =
				mapNewToOld.mapToPair( new WeightedArrays() );
		final JavaPairRDD< Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > > reducedArrays =
				weightedArrays.reduceByKey( new ReduceArrays() );
		final JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > result =
				reducedArrays.mapValues( new NormalizeBySumOfWeights() );

		return result;
	}

	public static class MatchCoordinates< V > implements
	PairFlatMapFunction< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > >
	{

		public static interface Matcher extends Serializable
		{
			public void call( Tuple2< Double, Double > newPointInOldCoordinates, Tuple2< Integer, Integer > newCoordinateGrid, Tuple2< Integer, Integer > oldCoordinateGrid, List< Tuple2< Tuple2< Integer, Integer >, Double > > associateWithNewCoordinatesGridAndWeights, int[] dim );
		}

		public static class NearestNeighborMatcher implements Matcher
		{

			@Override
			public void call( final Tuple2< Double, Double > newPointInOldCoordinates, final Tuple2< Integer, Integer > newCoordinateGrid, final Tuple2< Integer, Integer > oldCoordinateGrid, final List< Tuple2< Tuple2< Integer, Integer >, Double > > associateWithNewCoordinatesGridAndWeights, final int[] dim )
			{

				final long r1 = Math.round( newPointInOldCoordinates._1() );
				final long r2 = Math.round( newPointInOldCoordinates._2() );
				if ( r1 == oldCoordinateGrid._1().intValue() && r2 == oldCoordinateGrid._2().intValue() )
					associateWithNewCoordinatesGridAndWeights.add( Utility.tuple2( newCoordinateGrid, 1.0 ) );

			}
		}

		public static class NLinearMatcher implements Matcher
		{

			@Override
			public void call( final Tuple2< Double, Double > newPointInOldCoordinates, final Tuple2< Integer, Integer > newCoordinateGrid, final Tuple2< Integer, Integer > oldCoordinateGrid, final List< Tuple2< Tuple2< Integer, Integer >, Double > > associateWithNewCoordinatesGridAndWeights, final int[] dim )
			{

				final double diff1 = Math.abs( newPointInOldCoordinates._1() - oldCoordinateGrid._1().doubleValue() );
				final double diff2 = Math.abs( newPointInOldCoordinates._2() - oldCoordinateGrid._2().doubleValue() );
				if ( diff1 <= 1.0 && diff2 <= 1.0 )
					associateWithNewCoordinatesGridAndWeights.add( Utility.tuple2( newCoordinateGrid, ( 1.0 - diff1 ) * ( 1.0 - diff2 ) ) );

			}
		}

		private final Broadcast< ? extends List< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > > newCoordinatesToOldCoordinates;

		private final int[] dim;

		private final Matcher matcher;

		public MatchCoordinates( final Broadcast< ? extends List< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > > newCoordinatesToOldCoordinates, final int[] dim, final Matcher matcher )
		{
			this.newCoordinatesToOldCoordinates = newCoordinatesToOldCoordinates;
			this.dim = dim;
			this.matcher = matcher;
		}

		@Override
		public Iterator< Tuple2< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > > > call( final Tuple2< Tuple2< Integer, Integer >, V > t ) throws Exception
		{
			final Tuple2< Integer, Integer > oldCoordinateGrid = t._1();
			final ArrayList< Tuple2< Tuple2< Integer, Integer >, Double > > associations = new ArrayList<>();
			for ( final Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > c : newCoordinatesToOldCoordinates.getValue() )
			{
				final Tuple2< Integer, Integer > newCoordinateGrid = c._1();
				final Tuple2< Double, Double > newPointInOldCoordinates = c._2();
				final Tuple2< Double, Double > bounded = new Tuple2<>(
						Math.max( Math.min( newPointInOldCoordinates._1(), dim[ 0 ] - 1 ), 0 ),
						Math.max( Math.min( newPointInOldCoordinates._2(), dim[ 1 ] - 1 ), 0 ) );
				matcher.call( bounded, newCoordinateGrid, oldCoordinateGrid, associations, dim );
			}

			final Iterable< Tuple2< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > > > it = () -> {
				final Iterator< Tuple2< Tuple2< Integer, Integer >, Double > > it1 = associations.iterator();
				return new Iterator< Tuple2< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > > >()
				{
					@Override
					public boolean hasNext()
					{
						return it1.hasNext();
					}

					@Override
					public Tuple2< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > > next()
					{
						return Utility.tuple2( t, it1.next() );
					}

					@Override
					public void remove()
					{
						throw new UnsupportedOperationException();
					}
				};
			};
			return it.iterator();
		}
	}

	public static class SwapKey< V > implements
	PairFunction< Tuple2< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > >, Tuple2< Integer, Integer >, Tuple2< V, Double > >
	{
		@Override
		public Tuple2< Tuple2< Integer, Integer >, Tuple2< V, Double > > call( final Tuple2< Tuple2< Tuple2< Integer, Integer >, V >, Tuple2< Tuple2< Integer, Integer >, Double > > t ) throws Exception
		{
			final Tuple2< Integer, Integer > newCoord = t._2()._1();
			return Utility.tuple2( newCoord, Utility.tuple2( t._1()._2(), t._2()._2() ) );
		}
	}

	public static class WeightedArrays implements PairFunction< Tuple2< Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > >, Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > >
	{
		@Override
		public Tuple2< Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > >
		call( final Tuple2< Tuple2< Integer, Integer >, Tuple2< SparkInference.Variables, Double > > t ) throws Exception
		{
			final Tuple2< SparkInference.Variables, Double > arrWithWeight = t._2();
			final SparkInference.Variables vars = new SparkInference.Variables(
					arrWithWeight._1().coordinates.clone(),
					arrWithWeight._1().scalingFactors.clone(),
					arrWithWeight._1().estimate.clone() );
			final double weight = arrWithWeight._2();

			for ( int i = 0; i < vars.coordinates.length; ++i )
			{
				vars.coordinates[ i ] *= weight;
				vars.scalingFactors[ i ] *= weight;
			}

			for ( int i = 0; i < vars.estimate.length; ++i )
				vars.estimate[ i ] *= weight;

			return Utility.tuple2( t._1(), Utility.tuple2( vars, weight ) );
		}
	}

	public static class ReduceArrays implements
	Function2< Tuple2< SparkInference.Variables, Double >, Tuple2< SparkInference.Variables, Double >, Tuple2< SparkInference.Variables, Double > >
	{
		@Override
		public Tuple2< SparkInference.Variables, Double >
		call( final Tuple2< SparkInference.Variables, Double > t1, final Tuple2< SparkInference.Variables, Double > t2 ) throws Exception
		{
			final Variables vars1 = t1._1();
			final Variables vars2 = t2._1();
			for ( int i = 0; i < vars1.coordinates.length; ++i )
			{
				vars1.coordinates[ i ] += vars2.coordinates[ i ];
				vars1.scalingFactors[ i ] += vars2.scalingFactors[ i ];
			}

			for ( int i = 0; i < vars1.estimate.length; ++i )
				vars1.estimate[ i ] += vars2.estimate[ i ];

			return Utility.tuple2( vars1, t1._2() + t2._2() );
		}
	}

	public static class NormalizeBySumOfWeights implements
	Function< Tuple2< SparkInference.Variables, Double >, SparkInference.Variables >
	{
		@Override
		public SparkInference.Variables call( final Tuple2< SparkInference.Variables, Double > t ) throws Exception
		{
			final Variables vars = t._1();
			final double weight = t._2();
			for ( int i = 0; i < vars.coordinates.length; ++i )
			{
				vars.coordinates[ i ] /= weight;
				vars.scalingFactors[ i ] /= weight;
			}

			for ( int i = 0; i < vars.estimate.length; ++i )
				vars.estimate[ i ] /= weight;

			return vars;
		}
	}

	public static void main( final String[] args ) throws InterruptedException
	{
		final SparkConf conf = new SparkConf().setAppName( "InterpolationTest" ).setMaster( "local" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		final int[] dim = new int[] { 20, 20 };
		final int[] radii1 = new int[] { 5, 5 };
		final int[] radii2 = new int[] { 2, 2 };
		final int[] steps1 = new int[] { 5, 5 };
		final int[] steps2 = new int[] { 2, 2 };
		final int[] sourceDim = new int[] { 3, 3 };
		final CorrelationBlocks cbs1 = new CorrelationBlocks( radii1, steps1 );
		final CorrelationBlocks cbs2 = new CorrelationBlocks( radii2, steps2 );

		// create rdd with local coordinates of r = [5,5], s = [5,5] and values
		// =
		final ArrayList< CorrelationBlocks.Coordinate > init = cbs1.generateFromBoundingBox( dim );
		final JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > rdd = sc
				.parallelize( init )
				.mapToPair( coordinate -> {
					final Tuple2< Integer, Integer > wc = coordinate.getWorldCoordinates();
					return Utility.tuple2( coordinate.getLocalCoordinates(), new double[] { wc._1() + wc._2() * dim[ 0 ] } );
				} )
				.mapValues( c -> new SparkInference.Variables( c, c.clone(), c.clone() ) );

		final List< Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables > > rddCollected = rdd.collect();

		// map from cbs2 into cbs1
		final ArrayList< CorrelationBlocks.Coordinate > newCoords = cbs2.generateFromBoundingBox( dim );
		final ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< Double, Double > > > mapping = new ArrayList<>();
		for ( final CorrelationBlocks.Coordinate n : newCoords )
			mapping.add( Utility.tuple2( n.getLocalCoordinates(), cbs1.translateCoordinateIntoThisBlockCoordinates( n ) ) );

		// inteprolate using map from cbs2 into cbs1
		final JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > interpol = interpolate( sc, rdd, sc.broadcast( mapping ), sourceDim, new MatchCoordinates.NLinearMatcher() );

		final List< Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables > > interpolCollected = interpol.collect();

		final JavaPairRDD< Tuple2< Integer, Integer >, SparkInference.Variables > interpol2 = interpolate( sc, rdd, sc.broadcast( mapping ), sourceDim, new MatchCoordinates.NearestNeighborMatcher() );

		final List< Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables > > interpolCollected2 = interpol2.collect();

		sc.close();

		final ArrayImg< DoubleType, DoubleArray > sourceImg = ArrayImgs.doubles( sourceDim[ 0 ], sourceDim[ 1 ] );

		final ArrayRandomAccess< DoubleType > ra = sourceImg.randomAccess();
		for ( final Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables > rddC : rddCollected )
		{
			ra.setPosition( new int[] { rddC._1()._1(), rddC._1()._2() } );
			ra.get().set( rddC._2().coordinates[ 0 ] );
		}

		final ScaleAndTranslation tf = new ScaleAndTranslation( new double[] { steps1[ 0 ] * 1.0 / steps2[ 0 ], steps1[ 1 ] * 1.0 / steps2[ 1 ] }, new double[] { ( radii1[ 0 ] - radii2[ 0 ] ) * 1.0 / steps2[ 0 ], ( radii1[ 1 ] - radii2[ 1 ] ) * 1.0 / steps2[ 1 ] } );

		final RealTransformRandomAccessible< DoubleType, InverseRealTransform > transformed = RealViews.transform( Views.interpolate( Views.extendBorder( sourceImg ), new NLinearInterpolatorFactory< DoubleType >() ), tf );

		final RealTransformRandomAccessible< DoubleType, InverseRealTransform > transformed2 = RealViews.transform( Views.interpolate( Views.extendBorder( sourceImg ), new NearestNeighborInterpolatorFactory< DoubleType >() ), tf );

		Thread.sleep( 3000 );
		System.out.flush();
		System.out.println( interpolCollected.size() );
		System.out.println( "Comparing nlinear ..." );

		final RandomAccess< DoubleType > t = transformed.randomAccess();
		for ( final Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables > iCol : interpolCollected )
		{
			t.setPosition( new int[] { iCol._1()._1(), iCol._1()._2() } );
			if ( Math.abs( iCol._2().coordinates[ 0 ] - t.get().get() ) > 1e-10 )
				System.out.println( iCol._1() + Arrays.toString( iCol._2().coordinates ) + " " + t.get().get() );
		}

		System.out.flush();
		System.out.println( interpolCollected.size() );
		System.out.println( "Comparing nearest neighbor ..." );

		final RandomAccess< DoubleType > t2 = transformed2.randomAccess();
		for ( final Tuple2< Tuple2< Integer, Integer >, SparkInference.Variables > iCol : interpolCollected2 )
		{
			t2.setPosition( new int[] { iCol._1()._1(), iCol._1()._2() } );
			if ( Math.abs( iCol._2().coordinates[ 0 ] - t2.get().get() ) > 1e-10 )
				System.out.println( iCol._1() + Arrays.toString( iCol._2().coordinates ) + " " + t2.get().get() );
		}
	}

}
