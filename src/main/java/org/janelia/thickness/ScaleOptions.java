package org.janelia.thickness;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.janelia.thickness.inference.Options;

public class ScaleOptions
{
	public final int[][] steps;

	public final int[][] radii;

	public final int[][] correlationBlockRadii;

	public final int[][] maxOffsets;

	public final Options[] inference;

	public final int scale;

	public final String source;

	public final String target;

	public final int start;

	public final int stop;

	public final int joinStepSize;

	public final boolean[] logMatrices;

	public final int[] chunkSizes;

	public final int[] overlaps;

	public ScaleOptions(
			int[][] steps,
			int[][] radii,
			int[][] correlationBlockRadii,
			int[][] maxOffsets,
			Options[] inference,
			int scale,
			String source,
			String target,
			int start,
			int stop,
			int joinStepSize,
			boolean[] logMatrices,
			final int[] chunkSizes,
			final int[] overlaps )
	{
		super();
		this.steps = steps;
		this.radii = radii;
		this.correlationBlockRadii = correlationBlockRadii;
		this.maxOffsets = maxOffsets;
		this.inference = inference;
		this.scale = scale;
		this.source = source;
		this.target = target;
		this.start = start;
		this.stop = stop;
		this.joinStepSize = joinStepSize;
		this.logMatrices = logMatrices;
		this.chunkSizes = chunkSizes;
		this.overlaps = overlaps;
	}

	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb
				.append( "scale=" ).append( scale ).append( "\n" )
				.append( "source=" ).append( source ).append( "\n" )
				.append( "target=" ).append( target ).append( "\n" );
		for ( int i = 0; i < steps.length; ++i )
		{
			sb
					.append( "i=" ).append( i ).append( "\n" )
					.append( "\tradii=" ).append( Arrays.toString( radii[ i ] ) ).append( "\n" )
					.append( "\tsteps=" ).append( Arrays.toString( steps[ i ] ) ).append( "\n" )
					.append( "\topts=" ).append( inference[ i ].toString() );
		}
		return sb.toString();
	}

	public static ScaleOptions createFromFile( String path ) throws JsonIOException, JsonSyntaxException, FileNotFoundException
	{

		Gson gson = new Gson();

		JsonParser parser = new JsonParser();

		Options defaultOptions = Options.generateDefaultOptions();

		JsonObject defaultOptionsJson = parser.parse( gson.toJson( defaultOptions ) ).getAsJsonObject();

		File f = new File( path );
		JsonObject json = parser.parse( new FileReader( f ) ).getAsJsonObject();
//		System.out.println( defaultOptionsJson.toString() );
		JsonObject defaultOptionsFromUser = json.get( "defaultOptions" ).getAsJsonObject();
		for ( Entry< String, JsonElement > entry : defaultOptionsFromUser.entrySet() )
			defaultOptionsJson.add( entry.getKey(), entry.getValue() );

		int scale = json.get( "scale" ) == null ? 0 : json.get( "scale" ).getAsInt();
		String source = json.get( "source" ).getAsString();
		String target = json.get( "target" ).getAsString();

		int start = json.get( "start" ).getAsInt();
		int stop = json.get( "stop" ).getAsInt();

		JsonArray optionsRadiiAndSteps = json.get( "options" ).getAsJsonArray();

		int nIterations = optionsRadiiAndSteps.size();
		int[][] steps = new int[ nIterations ][];
		int[][] radii = new int[ nIterations ][];
		int[][] correlationBlockRadii = new int[ nIterations ][];
		int[][] maxOffsets = new int[ nIterations ][];
		Options[] opts = new Options[ nIterations ];

		int[] chunkSizes = new int[ nIterations ];
		int[] overlaps = new int[ nIterations ];

		for ( int i = 0; i < nIterations; ++i )
		{
			JsonObject tuple = optionsRadiiAndSteps.get( i ).getAsJsonObject();
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
				JsonObject currentOpts = tuple.get( "inference" ).getAsJsonObject();
				for ( Entry< String, JsonElement > entry : currentOpts.entrySet() )
					defaultOptionsJson.add( entry.getKey(), entry.getValue() );
			}

			opts[ i ] = gson.fromJson( defaultOptionsJson, Options.class );

			chunkSizes[ i ] = tuple.has( "chunkSize" ) ? Math.min( tuple.get( "chunkSize" ).getAsInt(), stop - start ) : stop - start;
			overlaps[ i ] = tuple.has( "overlap" ) ? Math.min( tuple.get( "overlap" ).getAsInt(), 2 * opts[ i ].comparisonRange ) : 2 * opts[ i ].comparisonRange;

		}

		int joinStepSize = Math.max( json.has( "joinStepSize" ) ? json.get( "joinStepSize" ).getAsInt() : 0, 2 * opts[ 0 ].comparisonRange );

		final boolean[] logMatrices = new boolean[ steps.length ];
		if ( json.has( "logMatrices" ) )
		{
			JsonElement logMatricesJson = json.get( "logMatrices" );
			if ( logMatricesJson.isJsonArray() )
			{
				for ( JsonElement val : logMatricesJson.getAsJsonArray() )
				{
					int i = val.getAsInt();
					if ( i >= 0 && i < logMatrices.length )
						logMatrices[ i ] = true;
				}
			}
			else if ( logMatricesJson.isJsonObject() )
			{
				for ( Entry< String, JsonElement > keyVal : logMatricesJson.getAsJsonObject().entrySet() )
				{
					int key = Integer.parseInt( keyVal.getKey() );
					if ( key >= 0 && key < logMatrices.length )
						logMatrices[ key ] = keyVal.getValue().getAsBoolean();
				}
			}
			else if ( logMatricesJson.getAsJsonPrimitive().isBoolean() )
			{
				Arrays.fill( logMatrices, logMatricesJson.getAsBoolean() );
			}
		}

		return new ScaleOptions(
				steps, radii, correlationBlockRadii,
				maxOffsets, opts, scale, source,
				target, start, stop, joinStepSize, logMatrices,
				chunkSizes, overlaps );
//		System.out.println( defaultOptionsJson.toString() );
	}

	public static void main( String[] args ) throws JsonIOException, JsonSyntaxException, FileNotFoundException
	{
//		String path = "/home/hanslovskyp/local/tmp/z_spacing_options.json";
		String path = "/home/hanslovskyp/workspace-spark/z_spacing-spark/example.json";
		ScaleOptions opts = createFromFile( path );
		System.out.println( opts );
	}

}
