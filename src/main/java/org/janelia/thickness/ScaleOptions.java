package org.janelia.thickness;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Map.Entry;

import org.janelia.thickness.inference.Options;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class ScaleOptions
{
	public final int[][] steps;

	public final int[][] radii;

	public final int[][] correlationBlockRadii;

	public final int[][] maxOffsets;

	public final Options[] inference;

	public final int scale;

	public final String source;

	public final String mask;

	public final String target;

	public final int start;

	public final int stop;

	public final boolean[] logMatrices;

	public final int joinStepSize;

	public ScaleOptions( final int[][] steps, final int[][] radii, final int[][] correlationBlockRadii, final int[][] maxOffsets, final Options[] inference, final int scale, final String source, final String mask, final String target, final int start, final int stop, final boolean[] logMatrices, final int joinStepSize )
	{
		super();
		this.steps = steps;
		this.radii = radii;
		this.correlationBlockRadii = correlationBlockRadii;
		this.maxOffsets = maxOffsets;
		this.inference = inference;
		this.scale = scale;
		this.source = source;
		this.mask = mask;
		this.target = target;
		this.start = start;
		this.stop = stop;
		this.logMatrices = logMatrices;
		this.joinStepSize = joinStepSize;
	}

	@Override
	public String toString()
	{
		final StringBuilder sb = new StringBuilder();
		sb.append( "scale=" ).append( scale ).append( "\n" ).append( "source=" ).append( source ).append( "\n" ).append( "target=" ).append( target ).append( "\n" );
		for ( int i = 0; i < steps.length; ++i )
			sb.append( "i=" ).append( i ).append( "\n" ).append( "\tradii=" ).append( Arrays.toString( radii[ i ] ) ).append( "\n" ).append( "\tsteps=" ).append( Arrays.toString( steps[ i ] ) ).append( "\n" ).append( "\topts=" ).append( inference[ i ].toString() );
		return sb.toString();
	}

	public static ScaleOptions createFromFile( final String path ) throws JsonIOException, JsonSyntaxException, FileNotFoundException
	{

		final Gson gson = new Gson();

		final JsonParser parser = new JsonParser();

		final Options defaultOptions = Options.generateDefaultOptions();

		final JsonObject defaultOptionsJson = parser.parse( gson.toJson( defaultOptions ) ).getAsJsonObject();

		final File f = new File( path );
		final JsonObject json = parser.parse( new FileReader( f ) ).getAsJsonObject();
		//		System.out.println( defaultOptionsJson.toString() );
		final JsonObject defaultOptionsFromUser = json.get( "defaultOptions" ).getAsJsonObject();
		for ( final Entry< String, JsonElement > entry : defaultOptionsFromUser.entrySet() )
			defaultOptionsJson.add( entry.getKey(), entry.getValue() );

		final JsonArray optionsRadiiAndSteps = json.get( "options" ).getAsJsonArray();

		final int nIterations = optionsRadiiAndSteps.size();
		final int[][] steps = new int[ nIterations ][];
		final int[][] radii = new int[ nIterations ][];
		final int[][] correlationBlockRadii = new int[ nIterations ][];
		final int[][] maxOffsets = new int[ nIterations ][];
		final Options[] opts = new Options[ nIterations ];

		for ( int i = 0; i < nIterations; ++i )
		{
			final JsonObject tuple = optionsRadiiAndSteps.get( i ).getAsJsonObject();
			radii[ i ] = gson.fromJson( tuple.get( "radii" ), int[].class );
			steps[ i ] = gson.fromJson( tuple.get( "steps" ), int[].class );

			if ( tuple.has( "correlationBlockRadii" ) )
				correlationBlockRadii[ i ] = gson.fromJson( tuple.get( "correlationBlockRadii" ), int[].class );
			else
				correlationBlockRadii[ i ] = radii[ i ].clone();

			if ( tuple.has( "maxOffsets" ) )
				maxOffsets[ i ] = gson.fromJson( tuple.get( "maxOffsets" ), int[].class );
			else
				maxOffsets[ i ] = new int[] { 0, 0 };

			if ( tuple.has( "inference" ) )
			{
				final JsonObject currentOpts = tuple.get( "inference" ).getAsJsonObject();
				for ( final Entry< String, JsonElement > entry : currentOpts.entrySet() )
					defaultOptionsJson.add( entry.getKey(), entry.getValue() );
			}

			opts[ i ] = gson.fromJson( defaultOptionsJson, Options.class );

		}

		final int scale = json.get( "scale" ) == null ? 0 : json.get( "scale" ).getAsInt();
		final String source = json.get( "source" ).getAsString();
		final String mask = json.get( "mask" ).getAsString();
		final String target = json.get( "target" ).getAsString();

		final int start = json.get( "start" ).getAsInt();
		final int stop = json.get( "stop" ).getAsInt();

		final boolean[] logMatrices = new boolean[ steps.length ];
		if ( json.has( "logMatrices" ) )
		{
			final JsonElement logMatricesJson = json.get( "logMatrices" );
			if ( logMatricesJson.isJsonArray() )
				for ( final JsonElement val : logMatricesJson.getAsJsonArray() )
				{
					final int i = val.getAsInt();
					if ( i >= 0 && i < logMatrices.length )
						logMatrices[ i ] = true;
				}
			else if ( logMatricesJson.isJsonObject() )
				for ( final Entry< String, JsonElement > keyVal : logMatricesJson.getAsJsonObject().entrySet() )
				{
					final int key = Integer.parseInt( keyVal.getKey() );
					if ( key >= 0 && key < logMatrices.length )
						logMatrices[ key ] = keyVal.getValue().getAsBoolean();
				}
			else if ( logMatricesJson.getAsJsonPrimitive().isBoolean() )
				Arrays.fill( logMatrices, logMatricesJson.getAsBoolean() );
		}
		else
			Arrays.fill( logMatrices, false );

		final int joinStepSize = Math.max( json.has( "joinStepSize" ) ? json.get( "joinStepSize" ).getAsInt() : 0, 2 * opts[ 0 ].comparisonRange );

		return new ScaleOptions( steps, radii, correlationBlockRadii, maxOffsets, opts, scale, source, mask, target, start, stop, logMatrices, joinStepSize );
		//		System.out.println( defaultOptionsJson.toString() );
	}

	public static void main( final String[] args ) throws JsonIOException, JsonSyntaxException, FileNotFoundException
	{
		//		String path = "/home/hanslovskyp/local/tmp/z_spacing_options.json";
		final String path = "/home/hanslovskyp/workspace-spark/z_spacing-spark/example.json";
		final ScaleOptions opts = createFromFile( path );
		System.out.println( opts );
	}

}
