//package org.janelia.thickness;
//
//
//
//
//import net.imglib2.*;
//import scala.Tuple2;
//
//import java.util.Map;
//
///**
// * Created by hanslovskyp on 11/19/15.
// */
//public class MapCellImg< T, M extends Map<Tuple2< Integer, Integer >, RandomAccessibleInterval<T> > > implements RandomAccessibleInterval<T> {
//
//    private final long[] min;
//    private final long[] max;
//    private final long[] dims;
//    private final long[] cellDims;
//    private final M cells;
//    private final int nDim = 2;
//
//    public MapCellImg(long[] min, long[] max, long[] cellDims, M cells) {
//        this.min = min;
//        this.max = max;
//        this.dims = new long[nDim];
//        for ( int d = 0; d < nDim; ++d )
//            this.dims[d] = max[d] - min[d] + 1;
//        this.cellDims = cellDims;
//        this.cells = cells;
//    }
//
//    public class CellRandomAccess extends Point implements RandomAccess<T>
//    {
//        private final long[] cellCoord = new long[2];
//        private final long[] withinCellCoord = new long[2];
//
//        private
//
//        public CellRandomAccess()
//        {
//            super(2);
//        }
//
//        @Override
//        public RandomAccess<T> copyRandomAccess() {
//            return null;
//        }
//
//        @Override
//        public T get() {
//            return null;
//        }
//
//        @Override
//        public Sampler<T> copy() {
//            return null;
//        }
//    }
//
//    @Override
//    public long min(int d) {
//        return min[d];
//    }
//
//    @Override
//    public void min(long[] min) {
//        System.arraycopy( this.min, 0, min, 0, nDim );
//    }
//
//    @Override
//    public void min(Positionable positionable) {
//        positionable.setPosition( min );
//    }
//
//    @Override
//    public long max(int d) {
//        return max[d];
//    }
//
//    @Override
//    public void max(long[] max) {
//        System.arraycopy( this.max, 0, max, 0, nDim );
//    }
//
//    @Override
//    public void max(Positionable positionable) {
//        positionable.setPosition( max );
//    }
//
//    @Override
//    public void dimensions(long[] dims) {
//        System.arraycopy( this.dims, 0, dims, 0, nDim );
//    }
//
//    @Override
//    public long dimension(int d) {
//        return dims[d];
//    }
//
//    @Override
//    public RandomAccess<T> randomAccess() {
//        return null;
//    }
//
//    @Override
//    public RandomAccess<T> randomAccess(Interval interval) {
//        return randomAccess();
//    }
//
//    @Override
//    public double realMin(int d) {
//        return min[d];
//    }
//
//    @Override
//    public void realMin(double[] min) {
//        for ( int d = 0; d < nDim; ++d )
//            min[d] = this.min[d];
//    }
//
//    @Override
//    public void realMin(RealPositionable min) {
//        min.setPosition( this.min );
//    }
//
//    @Override
//    public double realMax(int d) {
//        return max[d];
//    }
//
//    @Override
//    public void realMax(double[] max) {
//        for ( int d = 0; d < nDim; ++d )
//            max[d] = this.max[d];
//    }
//
//    @Override
//    public void realMax(RealPositionable max) {
//        max.setPosition( this.max );
//    }
//
//    @Override
//    public int numDimensions() {
//        return nDim;
//    }
//}
