package org.janelia.thickness.similarity;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.thickness.KryoSerialization;
import org.janelia.thickness.plugin.RealSumFloatNCC;
import org.janelia.thickness.utility.Utility;
import org.janelia.utility.MatrixStripConversion;
import org.junit.Assert;
import org.junit.Test;

import ij.ImagePlus;
import ij.process.FloatProcessor;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import scala.Tuple2;

/**
 * 
 * @author Philipp Hanslovsky &lt;hanslovskyp@janelia.hhmi.org&gt;
 *
 */
public class ComputeMatricesChunkedTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7583073104997866850L;

	@Test
	public void test() throws InterruptedException, ExecutionException {
		SparkConf conf = new SparkConf()
				.setAppName("Smaller Joins!")
				// Only use one CPU. Spark overhead kills test with heap growing bigger than 512MB if using all CPUs.
				.setMaster("local[1]")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.registrator", KryoSerialization.Registrator.class.getName())
				;
        JavaSparkContext sc = new JavaSparkContext(conf);
        final String pattern = "/groups/saalfeld/saalfeldlab/FROM_TIER2/hanslovskyp/flyem/data/Z0115-22_Sec27/align1/Thick/image.%05d.png";
        final int start = 3750;
        final int size = 80;
        final int stop = start + size;
        final int scaleLevel = 3;
        final int stepSize = 30;
        final int range = 15;
        ImagePlus firstImp = new ImagePlus( String.format( pattern, start ) );
        final int w = firstImp.getWidth();
        final int h = firstImp.getHeight();
        final int[] dim = {w / (1<<scaleLevel), h / (1<<scaleLevel)};
        int[] stride = new int[] { dim[0]/2, dim[1]/2 };
        int[] radius = {stride[0]/2, stride[0]/2};
        ArrayList<Integer> arange = Utility.arange(start, stop);
        @SuppressWarnings("serial")
		JavaPairRDD<Integer, FloatProcessor> files = sc.parallelize(arange)
                .mapToPair(new Utility.LoadFileFromPattern(pattern))
                .mapToPair(new PairFunction<Tuple2<Integer, FloatProcessor>, Integer, FloatProcessor>() {
                    @Override
                    public Tuple2<Integer, FloatProcessor> call(Tuple2<Integer, FloatProcessor> t) throws Exception {
                        return Utility.tuple2(t._1().intValue() - start, t._2());
                    }
                })
                .mapToPair(new Utility.DownSample<Integer>(scaleLevel))
                .cache()
                ;
        
        final FloatProcessor[] stack = new FloatProcessor[size];
        for ( final Tuple2< Integer, FloatProcessor > indexedFp : files.collect() )
        	stack[ indexedFp._1() ] = indexedFp._2();

        ComputeMatricesChunked test = new ComputeMatricesChunked(sc, files, stepSize, range, dim, true);

        Map<Tuple2<Integer, Integer>, FloatProcessor> strips = 
        		test.run(range, stride, radius).collectAsMap();
        sc.close();
        
        int count = 0;
        ExecutorService es = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        for ( int y = radius[1], yIndex = 0; y < dim[1]; y += stride[0], ++yIndex )
        {
        	for ( int x = radius[0], xIndex = 0; x < dim[0]; x += stride[0], ++xIndex )
        	{
        		Assert.assertTrue( 
        				String.format("test.containsKey( %d, %d )", xIndex, yIndex ), 
        				strips.containsKey( Utility.tuple2( xIndex, yIndex ) )
        				);
        		++count;
        		
        		final FloatProcessor strip = strips.get( Utility.tuple2( xIndex, yIndex ) );
        		final FloatProcessor matrixFp = new FloatProcessor( size, size );
        		matrixFp.add( Double.NaN );
        		ArrayList< Callable< Void > > tasks = new ArrayList<>();
    			final int minX = Math.max( x - radius[0], 0 );
    			final int minY = Math.max( y - radius[1], 0 );
    			final int maxX = Math.min( x + radius[0], dim[0] );
    			final int maxY = Math.min( y + radius[1], dim[1] );
    			final int sizeX = maxX - minX;
    			final int sizeY = maxY - minY;
        		final FloatProcessor[] localStack = new FloatProcessor[ stack.length ];
        		for ( int m = 0; m < stack.length; ++m )
        		{
        			final FloatProcessor fp = new FloatProcessor( sizeX, sizeY );
        			final FloatProcessor source = stack[ m ];
        			for ( int localY = 0; localY < sizeY; ++localY )
        				for ( int localX = 0; localX < sizeX; ++localX )
        					fp.setf( localX,  localY, source.getf( localX + minX, localY + minY ) );
        			localStack[ m ] = fp;
        		}
        		for ( int i = 0; i < size; ++i )
        		{
        			final int finalI = i;
        			tasks.add( () -> {
            			matrixFp.setf( finalI, finalI, 1.0f );
        				for ( int k = finalI + 1; k - finalI <= range && k < size; ++k )
        				{
        					final float val = new RealSumFloatNCC( (float[])localStack[finalI].getPixels(), (float[])localStack[k].getPixels()).call().floatValue();
        					matrixFp.setf( finalI, k, val );
        					matrixFp.setf( k, finalI, val );
        				}
        				return null;
        			});
        		}
        		List<Future<Void>> futures = es.invokeAll( tasks );
        		for ( Future< Void > f : futures )
        		{
        			f.get();
        		}

        		RandomAccessibleInterval<FloatType> stripRef = MatrixStripConversion.matrixToStrip( ImageJFunctions.wrapFloat( new ImagePlus( "", matrixFp ) ), range, new FloatType() );
        		Img<FloatType> stripComp = ImageJFunctions.wrapFloat( new ImagePlus( "", strip ) );
        		assertEquals( stripRef.numDimensions(), stripComp.numDimensions() );
        		for ( int d = 0; d < stripRef.numDimensions(); ++d )
        			assertEquals( stripRef.dimension( d ), stripComp.dimension( d ) );
        		
        		for ( Cursor< FloatType > refC = Views.iterable( stripRef ).cursor(), compC = stripComp.cursor(); refC.hasNext(); )
        		{
        			float ref = refC.next().get();
        			float comp = compC.next().get();
        			
        			if ( Float.isNaN( ref ) )
        				Assert.assertTrue( Float.isNaN( comp ) );
        			else
        				Assert.assertEquals( ref, comp, 1e-6f );
        			
        		}
        		
        		
        	}
        }
        es.shutdown();
        
        assertEquals( count, strips.size() );
	}
	

}
