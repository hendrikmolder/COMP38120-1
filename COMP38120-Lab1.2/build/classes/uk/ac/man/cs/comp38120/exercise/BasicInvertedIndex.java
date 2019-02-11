/**
 * Basic Inverted Index
 * 
 * This Map Reduce program should build an Inverted Index from a set of files.
 * Each token (the key) in a given file should reference the file it was found 
 * in. 
 * 
 * The output of the program should look like this:
 * sometoken [file001, file002, ... ]
 * 
 * @author Kristian Epps
 */
package uk.ac.man.cs.comp38120.exercise;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.man.cs.comp38120.io.array.ArrayListWritable;
import uk.ac.man.cs.comp38120.io.map.String2FloatOpenHashMapWritable;
import uk.ac.man.cs.comp38120.io.pair.PairOfWritables;
import uk.ac.man.cs.comp38120.io.triple.TripleOfIntsString;
import uk.ac.man.cs.comp38120.ir.Stemmer;
import uk.ac.man.cs.comp38120.ir.StopAnalyser;
import uk.ac.man.cs.comp38120.util.XParser;

public class BasicInvertedIndex extends Configured implements Tool
{
    private static final Logger LOG = Logger
            .getLogger(BasicInvertedIndex.class);

    /* 
     * @returns A TripleOfIntsString in the format (TF, totalTokens, token)
     */
    public static class Map extends 
            Mapper<Object, Text, Text, PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>>>
    {

        // INPUTFILE holds the name of the current file
        private final static Text INPUTFILE = new Text();
        
        // TOKEN should be set to the current token rather than creating a 
        // new Text object for each one
        @SuppressWarnings("unused")
        private final static Text TOKEN = new Text();

        // The StopAnalyser class helps remove stop words
        @SuppressWarnings("unused")
        private StopAnalyser stopAnalyser = new StopAnalyser();
                        
        // The stem method wraps the functionality of the Stemmer
        // class, which trims extra characters from English words
        // Please refer to the Stemmer class for more comments
        @SuppressWarnings("unused")
        private String stem(String word)
        {
            Stemmer s = new Stemmer();

            // A char[] word is added to the stemmer with its length,
            // then stemmed
            s.add(word.toCharArray(), word.length());
            s.stem();

            // return the stemmed char[] word as a string
            return s.toString();
        }
        
        // This method gets the name of the file the current Mapper is working
        // on
        @Override
        public void setup(Context context)
        {
            String inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String[] pathComponents = inputFilePath.split("/");
            INPUTFILE.set(pathComponents[pathComponents.length - 1]);
        }
        
        /* Check if string has more than 1 uppercase letters, in that case
         * leave the string in original form, else return it in lowercase.
         * Example: David vs david (still understandable when lowercase)
         * Example: DVD, NHL, BBC vs dvd, nhl, bbc 
         */
        private String caseFolding(String token) {
        	Integer countOfUpperCaseLetters = 0;
        	for (Character c : token.toCharArray()) {
        		if (Character.isUpperCase(c)) countOfUpperCaseLetters++;
        		if (countOfUpperCaseLetters > 1) return token;
        	}
        	return token.toLowerCase();
        }
         
        // TODO
        // This Mapper should read in a line, convert it to a set of tokens
        // and output each token with the name of the file it was found in
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
        	/* Get the filename from the first line */        	
        	String line = value.toString().trim();
        	HashMap<String, ArrayListWritable<IntWritable>> map = new HashMap<String, ArrayListWritable<IntWritable>>();
        	PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>> pair = new PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>>();
        	Text text = new Text();
			IntWritable position = new IntWritable(0);
			TripleOfIntsString keyTriple = new TripleOfIntsString();
			
   
        	/* Tokenize the text file */
        	StringTokenizer st = new StringTokenizer(line, " '\",;:.()[]{}!?/#$&^%£|=");
        	Integer tokenCount = st.countTokens();
        	if (tokenCount < 1) tokenCount = 1;
        	
        	/* Iterate over set of tokens and output them with the key */
        	while (st.hasMoreTokens()) {
        		String token = st.nextToken();  
        		position.set(position.get() + 1);;
        		
        		/* Check if token is a stop word */
        		if (!StopAnalyser.isStopWord(token)) {
        			
        			/* Do case folding */
        			token = caseFolding(token);

	        		/* Stem the word */
	        		token = stem(token);
	        		
	        		/* Insert to map */
	        		if (map.containsKey(token)) {
	        			IntWritable p = new IntWritable(position.get());
	        			map.get(token).add(p);
	        		} else {
	        			ArrayListWritable<IntWritable> pos = new ArrayListWritable<IntWritable>();
	        			IntWritable p = new IntWritable(position.get());
	        			pos.add(p);
	        			map.put(token, pos);
	        		}
        		} /* end-if not stopword */
        	}
        	
        	// http://lintool.github.io/MapReduceAlgorithms/
        	
        	for (String s : map.keySet()) {
        		/* KeyTriple: (count of a particular token; count of all tokents; file name) */
        		keyTriple.set(map.get(s).size(), tokenCount, INPUTFILE.toString());
        		text.set(s);
        		pair.set(keyTriple, map.get(s));
        		context.write(text, pair);
        	}
        }
    }

    public static class Reduce extends Reducer<Text, 
    			PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>>, 
				Text, 
				ArrayListWritable<PairOfWritables<PairOfWritables<Text, String2FloatOpenHashMapWritable>, ArrayListWritable<IntWritable>>>>
    {

    	Integer TOTAL_NUM_FILES = 6;
    	
    	// This Reduce Job should take in a key and an iterable of file names
        // It should convert this iterable to a writable array list and output
        // it along with the key
        public void reduce(
                Text key,
                Iterable<PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>>> values,
                Context context) throws IOException, InterruptedException
        {
        	
        	Integer countOfFilesTokenAppearsIn = 0;
        	/* The result array of all occurrences of the token */
        	ArrayListWritable<PairOfWritables<PairOfWritables<Text, String2FloatOpenHashMapWritable>, ArrayListWritable<IntWritable>>> occurences 
        		= new ArrayListWritable<PairOfWritables<PairOfWritables<Text, String2FloatOpenHashMapWritable>, ArrayListWritable<IntWritable>>>();
        			
        	Double tf, idf;

        	/* Loop through the occurrences of a token */
        	for (PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>> pair : values) {
        		/* A copy of the Pair passed from the Mapper to avoid Hadoop issues */
            	PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>> pairCopy 
        			= new PairOfWritables<TripleOfIntsString, ArrayListWritable<IntWritable>>();
        		/* A copy of the Pair from the Pair passed from Mapper to avoid Hadoop issues */
            	PairOfWritables<Text, String2FloatOpenHashMapWritable> nameAndMetaData 
        			= new PairOfWritables<Text, String2FloatOpenHashMapWritable>(); 
        		/* Result Pair that will be added to the occurrences */
            	PairOfWritables<PairOfWritables<Text, String2FloatOpenHashMapWritable>, ArrayListWritable<IntWritable>> resultPair 
        			= new PairOfWritables<PairOfWritables<Text, String2FloatOpenHashMapWritable>, ArrayListWritable<IntWritable>>();
            	
        		countOfFilesTokenAppearsIn++;
        		pairCopy.set(pair.getLeftElement(), pair.getRightElement());
        		String2FloatOpenHashMapWritable metadata = new String2FloatOpenHashMapWritable();
        		Text filename = new Text();
        		filename.set(pairCopy.getLeftElement().getRightElement());

        		tf 		= (double) pairCopy.getLeftElement().getLeftElement() / pairCopy.getLeftElement().getMiddleElement();
        		        		
        		/* Populate metadata */
        		metadata.put("tf", new Float(tf));        		
        		nameAndMetaData.set(filename, metadata);
        		
        		/* Add result result */
        		resultPair.set(nameAndMetaData, pairCopy.getRightElement());
        		occurences.add(resultPair);
        	}
        	
        	idf 	=  Math.log((double) TOTAL_NUM_FILES / countOfFilesTokenAppearsIn);
    		
        	/* Update the TF-IDF and IDF */
        	for (PairOfWritables<PairOfWritables<Text, String2FloatOpenHashMapWritable>, ArrayListWritable<IntWritable>> updatePair : occurences) {
        		updatePair.getLeftElement().getRightElement().put("tfidf", new Float(updatePair.getLeftElement().getRightElement().get("tf") * idf));
        		updatePair.getLeftElement().getRightElement().put("idf", new Float(idf));
        	}
        	
        	/* Emit results */	
        	context.write(key, occurences);        	
        }
    }

    // Lets create an object! :)
    public BasicInvertedIndex()
    {
    }

    // Variables to hold cmd line args
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception
    {
        
        // Handle command line args
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline = null;
        CommandLineParser parser = new XParser(true);

        try
        {
            cmdline = parser.parse(options, args);
        }
        catch (ParseException exp)
        {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            System.err.println(cmdline);
            return -1;
        }

        // If we are missing the input or output flag, let the user know
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT))
        {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        // Create a new Map Reduce Job
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
                .parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        // Set the name of the Job and the class it is in
        job.setJobName("Basic Inverted Index");
        job.setJarByClass(BasicInvertedIndex.class);
        job.setNumReduceTasks(reduceTasks);
        
        // Set the Mapper and Reducer class (no need for combiner here)
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        // Set the Output Classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfWritables.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayListWritable.class);

        // Set the input and output file paths
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // Time the job whilst it is running
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        // Returning 0 lets everyone know the job was successful
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new BasicInvertedIndex(), args);
    }
}
