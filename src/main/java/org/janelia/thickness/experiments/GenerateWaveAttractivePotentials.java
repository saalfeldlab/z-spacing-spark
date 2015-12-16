package org.janelia.thickness.experiments;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import net.imglib2.Cursor;
import net.imglib2.RealPoint;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import org.janelia.thickness.utility.Utility;
import org.janelia.thickness.lut.SingleDimensionLUTGrid;
import org.janelia.thickness.lut.SingleDimensionLUTRealTransformField;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanslovskyp on 10/23/15.
 */
public class GenerateWaveAttractivePotentials {

    // value between 0 and 1, monotonously decreasing
    public static interface Potential
    {
        public double value( double z );
        public double getZ0();
        public double getRange();
    }

    public static class GaussianPotential implements Potential
    {
        private final double z0;
        private final double sigma;
        private final double halfOverSigmaSquared;
        private final double range;

        public GaussianPotential(double z0, double sigma,double range) {
            this.z0 = z0;
            this.sigma = sigma;
            this.halfOverSigmaSquared = 0.5 / ( this.sigma * this.sigma );
            this.range = range;
        }

        public double value( final double z )
        {
            double diff = z - z0;
            return Math.exp( -diff*diff*halfOverSigmaSquared );
        }

        @Override
        public double getZ0() {
            return z0;
        }

        @Override
        public double getRange() {
            return range;
        }
    }

    public static class GaussianPotentialWithSinus implements Potential
    {
        private final GaussianPotential p;
        private final double[] periods;
        private final double[] phases;
        private final double z0;
        private final double range;

        public GaussianPotentialWithSinus(double z0, double sigma, double[] periods, double[] phases, double range) {
            this.p = new GaussianPotential( z0, sigma, range );
            this.periods = periods;
            this.phases = phases;
            this.z0 = z0;
            this.range = range;
        }

        @Override
        public double value(double z) {
            double diff = z - z0;
            double expVal = p.value(z);
            double weightStep = diff / range;
            double weightSum = 0.0;
            for( int i = 0; i < periods.length; ++i )
            {
//                double sinVal = 0.5 + 0.5 * Math.sin(periods[i] * diff + phases[i]);
//                expVal += sinVal;
//                weightSum += sinVal;

//                double w = weightStep * i;
//                expVal += w*Math.sin( periods[i]*diff + phases[i] );
//                weightSum += i;

                expVal *= Math.sin( periods[i]*diff + phases[i] );
            }
//            return expVal/(1+weightSum);
            return expVal;
        }

        @Override
        public double getZ0() {
            return z0;
        }

        @Override
        public double getRange() {
            return range;
        }
    }

    public static void main(String[] args) throws InterruptedException {



        HashMap<Tuple2<Long,Long>,List<Potential>> potentials = new HashMap<>();



//        long[] dim = new long[] { 200, 125, 240 };
        long[] dim = new long[]{199, 124, 2070};
        ArrayImg<DoubleType, DoubleArray> grid = ArrayImgs.doubles(dim);
        ArrayImg<DoubleType, DoubleArray> gridDeformation = ArrayImgs.doubles(dim);

        for( ArrayCursor<DoubleType> c = grid.cursor(); c.hasNext(); )
        {
            c.fwd();
            c.get().set( c.getDoublePosition( 2 ) );
        }


        long zRef2 = dim[2] / 3;
        long zRef1 = dim[2] * 2 / 3;
        long zRef3 = dim[2] / 2;

        Random rng = new Random(100);

        int nWaves = 100;

        double[][] directions = new double[nWaves][];
        for ( int n = 0; n < nWaves; ++n )
        {
            directions[n] = new double[] { Math.abs( rng.nextGaussian() * 50 + 50 ), Math.abs( rng.nextGaussian() * 25 + 25 ) };
        }

//        double[][] directions = new double[][]{
//                {3,1}, {1,1}, {3,2}, {1,4}, {3,3}, {10,0.1}, {0.1,10},
//                {1,1}, {1,1}, {2,1}, {2,4}, {4,3}, {10,4}, {12,10},
//                {3,1}, {1,1}, {3,2}, {1,4}, {3,3}, {10,0.1}, {0.1,10},
//                {1,1}, {1,1}, {2,1}, {2,4}, {4,3}, {10,4}, {12,10},
//                {1,1}, {1,1}, {2,1}, {2,4}, {4,3}, {10,4}, {12,10},
//                {3,1}, {1,1}, {3,2}, {1,4}, {3,3}, {10,0.1}, {0.1,10},
//                {1,1}, {1,1}, {2,1}, {2,4}, {4,3}, {10,4}, {12,10}
//        };



        double[] directionWeights = new double[ directions.length ];
        double[] offsets = new double[ directions.length];
        double[] sigmas = new double[ directions.length ];
        for ( int i = 0; i < directions.length; ++i )
        {
            directionWeights[i] = Math.abs( rng.nextGaussian()*0.10 + 0.15 );
            offsets[i] = dim[2] *1.0/ nWaves*(i+0.5*rng.nextDouble());
            sigmas[i] = 10*rng.nextDouble() + 27;
        }





        double previousGaussian = 0.0;
        for ( Cursor<RealComposite<DoubleType>> c = Views.flatIterable( Views.collapseReal( gridDeformation ) ).cursor(); c.hasNext(); )
        {
            c.fwd();
            long x = c.getLongPosition(0);
            long y = c.getLongPosition(1);
//            double gaussian = rng.nextGaussian();
            ArrayList<Potential> l = new ArrayList<>();
            long stepSize = dim[2] / nWaves;
            for( int n = 0; n < nWaves; ++n )
            {
                double off = offsets[n];//1.5*(rng.nextDouble() -0.5 ) *1.0/ nWaves*n + dim[2] *1.0/ nWaves*n+10;
                double[] dir = directions[n];
                double w = directionWeights[n];
                double main = (dir[0] * x + dir[1] * y) * 1.0 / (dir[0]+dir[1]);
                double rev = (dir[1] * x + dir[0] * y) * 1.0 / (dir[0]+dir[1]);
                GaussianPotential pot = new GaussianPotential(
                        off + w*main + (x * y) * 1.0 / (dim[0] * dim[1]) * Math.sin(0.1*main) + Math.cos(2.0 * x / dim[0]) + (x * y) * 2.0 / (dim[0] * dim[1]) * Math.sin(0.01*main),
                        sigmas[n], 100);
                l.add( pot );
            }
//            GaussianPotential potential1 = new GaussianPotential(
//                    zRef1+0.08*0.5*(3*y+x)+(3*x*y)*1.0/(3*dim[0]*dim[1])*Math.sin( 0.1*(3*x+y)/4 ) + Math.cos(2.0*x/dim[0])+(x*3*y)*2.0/(dim[0]*3*dim[1])*Math.sin( 0.01*(x+3*y)/4 ),
//                    18.5, 7);
//            GaussianPotential potential2 = new GaussianPotential(zRef2+0.2*0.5*(3*x+y)+Math.cos( 0.15*(x+y) ), 18.5);
//            GaussianPotential potential3 = new GaussianPotential(zRef3+0.5*0.5*(3*x+5*y)+2*Math.cos( 0.35*(x+y) ), 40.0 );
//            GaussianPotentialWithSinus potential1 = new GaussianPotentialWithSinus(zRef1+0.08*0.5*(3*y+x), 18.5, new double[]{10}, new double[1], 3);
//            previousGaussian = gaussian;
//            l.add( potential1 );
//            l.add( potential2 );
//            l.add( potential3 );
            potentials.put( Utility.tuple2( x, y ), l );
        }


        for ( Cursor<RealComposite<DoubleType>> c = Views.flatIterable( Views.collapseReal( gridDeformation ) ).cursor(); c.hasNext(); )
        {
            c.fwd();
            long x = c.getLongPosition(0);
            long y = c.getLongPosition(1);
            List<Potential> l = potentials.get(Utility.tuple2(x, y));
            RealComposite<DoubleType> col = c.get();
            for( long z = 0; z < dim[2] - 1; ++z )
            {
                DoubleType pos = col.get(z);
                int count = 0;
                for( Potential p : l )
                {
                    double diff = z - p.getZ0();
//                    if ( Math.abs(diff) > p.getRange() )
//                        continue;
                    if ( p.getZ0() >= z || z - p.getZ0() > p.getRange() )
                        continue;
                    double shift = -0.6*p.value(z)*(z-p.getZ0());
                    pos.add( new DoubleType( shift ) );
                    ++count;
                }
//                if ( count > 0 )
//                    pos.mul( 1.0 / count );
            }
        }

        for ( ArrayCursor<DoubleType> g = grid.cursor(), d = gridDeformation.cursor(); g.hasNext();  )
        {
            g.next().add( d.next() );
        }

        new ImageJ();
        ImagePlus imp = ImageJFunctions.wrapFloat(gridDeformation, "grid deformation");
        imp.setDimensions( 1, (int)dim[2], 1 );
        imp.show();
        ImagePlus impGrid = ImageJFunctions.wrapFloat(grid, "grid");
        impGrid.setDimensions( 1, (int)dim[2], 1 );
        impGrid.show();

        ArrayImg<DoubleType, DoubleArray> inverse = ArrayImgs.doubles(dim);



        int nThreads = Runtime.getRuntime().availableProcessors();
        final long maxZ = dim[2] - 1;
        ArrayList<Callable<Void>> jobs = new ArrayList<Callable<Void>>();
        for ( Cursor<RealComposite<DoubleType>> i = Views.flatIterable( Views.collapseReal( inverse ) ).cursor(); i.hasNext(); )
        {
            final RealComposite<DoubleType> iCol = i.next();
            final ArrayRandomAccess<DoubleType> ra = inverse.randomAccess();
            final RealPoint p = new RealPoint( 3 );
            ra.setPosition( i.getLongPosition( 0 ), 0 );
            ra.setPosition( i.getLongPosition( 1 ), 1 );
            ra.setPosition( maxZ, 2 );
            ra.get().set( maxZ );
            final SingleDimensionLUTRealTransformField transform = new SingleDimensionLUTRealTransformField(3, 3, grid);
            final SingleDimensionLUTGrid transform2 = new SingleDimensionLUTGrid(3, 3, grid, 2);
            jobs.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    RealPoint t = new RealPoint( new double[] { 0, 0, 0 } );
//                    System.out.println(p + " " + maxZ);
                    for (int z = 0; z < maxZ; ++z) {
                        ra.setPosition(z, 2);
                        transform.applyInverse(p, ra);
//                        System.out.println( t + " " + p  );
                        ra.get().set( p.getDoublePosition( 2 ) );
                    }
                    return null;
                }
            });
        }

        ExecutorService es = Executors.newFixedThreadPool(nThreads);
        es.invokeAll( jobs );

        ImagePlus impInverse = ImageJFunctions.wrapFloat(inverse, "inverse");
        impInverse.setDimensions( 1, (int)dim[2], 1 );
        impInverse.show();
        IJ.run("Reslice [/]...", "output=1.000 start=Top avoid");
        IJ.run("Convolve...", "text1=-0.5\n0\n0.5\n stack");
        IJ.run("Reslice [/]...", "output=1.000 start=Top avoid");
    }

}
