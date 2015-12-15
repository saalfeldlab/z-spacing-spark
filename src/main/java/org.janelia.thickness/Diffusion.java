package org.janelia.thickness;

import net.imglib2.algorithm.pde.PeronaMalikAnisotropicDiffusion;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.basictypeaccess.array.DoubleArray;
import net.imglib2.type.numeric.real.DoubleType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by hanslovskyp on 9/29/15.
 */
public class Diffusion {

    private final PeronaMalikAnisotropicDiffusion.DiffusionFunction func;
    private final double deltat;

    public Diffusion(PeronaMalikAnisotropicDiffusion.DiffusionFunction func, double deltat) {
        this.func = func;
        this.deltat = deltat;
    }


    public <K> JavaPairRDD< K, DPTuple > diffuse( JavaSparkContext sc, JavaPairRDD< K, DPTuple > input )
    {
        return input.mapToPair( new Diffuser<K>() );
    }

    public class Diffuser<T> implements PairFunction<Tuple2< T, DPTuple >, T, DPTuple >
    {

        @Override
        public Tuple2<T, DPTuple> call(Tuple2<T, DPTuple> t) throws Exception {
            ArrayImg<DoubleType, DoubleArray> img = t._2().createDoubleArrayImg();
            PeronaMalikAnisotropicDiffusion<DoubleType> diffusion =
                    new PeronaMalikAnisotropicDiffusion<DoubleType>(img, deltat, func);
            return null;
        }
    }

    public static class IgnoreNanDiffusionFunctionWrapper implements PeronaMalikAnisotropicDiffusion.DiffusionFunction
    {
        private final PeronaMalikAnisotropicDiffusion.DiffusionFunction func;

        public IgnoreNanDiffusionFunctionWrapper(PeronaMalikAnisotropicDiffusion.DiffusionFunction func) {
            this.func = func;
        }

        @Override
        public double eval(double v, long[] longs) {
            if ( Double.isNaN( v ) )
                return 0.0;
            else
                return this.func.eval( v, longs );
        }
    }

}
