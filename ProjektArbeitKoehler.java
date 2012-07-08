package org.myorg;
/*
 * 
 * 
 * 
 * TODO:
 * - Umgang mit Subdirectories; rekursives lesen v. Pfaden
 * - Transformation: s/\n/s/g (ersetze Zeilenumbrüche durch Leerzeichen)
 * 
 * 
 */




import java.io.IOException;
import java.util.*;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;


public class ProjektArbeitKoehler extends Configured implements Tool {
		
	/* TODO: fix me */
	 public class NonSplittableTextInputFormat extends TextInputFormat {
		@Override
		protected boolean isSplitable(FileSystem fs, Path file) {
			return false;
		}
		
		public NonSplittableTextInputFormat () {
			super();
		}
	}
	 
	/* 
	 * <Task1>: 
	 * WordCount
	 * - simple word count taken from http://hadoop.apache.org/common/docs/r1.0.2/mapred_tutorial.html
	 *
	 * @version: 1
	 * */
	public static class WordCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	 private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();
	  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
		  word.set(tokenizer.nextToken());
		  output.collect(word, one);
		}
	  }
	}
	public static class WordCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
		  sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	  }
	}
	/* </Task1> */
	 
	 /*
	  * <Task2>
	  * Kookkurrenz mit Pairs
	  */
	public static class KookkurrenzMitPairsMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final Text pair = new Text();
		private final IntWritable one = new IntWritable(1);
		private int window = 2;
		public void map(LongWritable key, Text line, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String text = line.toString();

			String[] terms = text.split("\\s+");
			java.util.Arrays.sort(terms);

			for (int i = 0; i < terms.length-1; i++) { // Iteration über jeden gefundenen Term
				String term = terms[i];
				for (int j = i+1; j < terms.length; j++) {	//
					String term2 = terms[j];
					boolean found = true;
					
					int compare = term.compareTo(term2);  
					if (compare < 0)  
					{  
						pair.set(term+", "+term2);
					}  
					else if (compare > 0)  
					{  
						pair.set(term2+", "+term);
					}  
					else  
					{  
						found = false;
					}  
					
					if (found) { // we only add a new key entry if term & term2 are different
						output.collect(pair, one);
					}
				}
			}
		}
	}
	public static class KookkurrenzMitPairsReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	/*</Task2> */

	 /*
	  * <Task3>
	  * Kookkurrenz mit Stripes
	  */
	public static class KookkurrenzMitStripesMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritable> {
		private final Text key = new Text();
		private final IntWritable one = new IntWritable(1);
		private int window = 2;
		
		public void map(LongWritable key, Text line, OutputCollector<Text, MapWritable> output, Reporter reporter) throws IOException {
			/* 
			 * Notwendiges Format für Stripes-Algorithmus
			 * term -> %hm{"Wort1"->1; "Wort2"->4, ...} */
			
			String text = line.toString();
			
			// HashMap als Hilfe zum Aufsummieren d. Werte d. Kookkurrenzmatrix
			HashMap<Text, IntWritable> hm = new HashMap<Text, IntWritable>();
			
			String[] terms = text.split("\\s+");

			for (int i = 0; i < terms.length; i++) { // Iteration über jeden gefundenen Term
				Text term = new Text(terms[i]);  // Format term: Text 
				
				if (term.getLength() == 0) 
					continue;
															
				for (int j = 0; j < terms.length; j++) {	// innere, geschachtelte Schleife über alle Terme
					Text term2 = new Text(terms[j]);
									
					if (i == j)  // Skip wenn Iteration an der selben Stelle steht, damit ein Termvorkommen nur einmal gezählt wird
						continue;
						
					if (hm.containsKey(term2)) { // Prüfen: ist term2 bereits in %hm
							IntWritable x = hm.get(term2);
							hm.put(term2, new IntWritable(hm.get(term2).get() + 1)); // erhöhen d. Counters
					}
					else { 
							hm.put(term2, one);
					}
				}
				
				// Hilfs-HashMap in MapWritable verpacken
				MapWritable stripe = new MapWritable(); // stripe: MapWritable zur Datenübergabe Map->Reduce
				MapWritable WritableHm = new MapWritable(); // WritableHm: Hilfs-MapWritable zur Umwandlung v. HashMap hm in MapWritable
				WritableHm.putAll(hm);
				hm.clear(); // Leeren der Hilfs-HashMap
				stripe.put(term, WritableHm); // schreibe term-> %hm in MapWritable
				// Schreibe der Zeile d. Kookkurrenzmatrix in OutputCollector
				output.collect(term,stripe);
			}
		}
	}	  
	public static class KookkurrenzMitStripesReduce extends MapReduceBase implements Reducer<Text, MapWritable, Text, Text> {
		public void reduce(Text key, Iterator<MapWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int sum = 0;
			// HashMap zum zählen erstellen
			HashMap<Text, Integer> hm = new HashMap<Text, Integer>();
		
			while (values.hasNext()) { // Iteration über Key->Value Input d. Reducer's
					MapWritable outterMapWritable = values.next();
					
					for ( Writable outerElemet : outterMapWritable.keySet() ) {  // Iteration durch äußeres MapWritable Objekt
						MapWritable innerMapWritable = (MapWritable) outterMapWritable.get(outerElemet);
						
						for ( Writable innerElement : innerMapWritable.keySet() ) {  // Iteration durch inneres MapWritable Objekt
								Text value = new Text();
								value.set(""+innerElement);
								if (hm.containsKey(value)) { // Prüfen: ist Value bereits in %hm
										hm.put(value, hm.get(value)+ 1);
								}
								else {
										hm.put(value, 1);
								}
								// TODO: delete me after Beschreibung Reducer 
								//tmpText.set(tmpText + "; "+innerElement+"->"+ innerMapWritable.get(innerElement));
								//ergibt: 
								// 		ipsum	; dolor->1; lorem->1; amet->1; lorem->1
								// 		lorem	; amet->1; ipsum->1; dolor->1; ipsum->1
						}
					}
			}
			output.collect(key, new Text(hm.toString()));
			//output.collect(key, new Text("lala"));
			
		}
	}	
	/*</Task3> */

	/*
	 * <run>
	 * 
	 * @param args Kommandozeilenparameter; Syntax: (String)Anwendungsfalls (String)Input-Dir (String)Output-Dir
	 **/
	public int run(String[] args) throws Exception {
		/* Prüfen ob geforderte Mindestanzahl an Parametern übergeben wurde */
		if (args.length != 3) {
			printUsage();
			return 1;
		}
		/* Werte aus args aufbereiten */		
		String useCase = args[0];
		String inputPath = args[1];
		String outputPath = args[2];
	
		/* Löschen eines vorhandenen "outputPath" Verzeichnis */
		deleteOldOutput(outputPath);		
		
		/* globale Konfiguration */
		JobConf conf = new JobConf(ProjektArbeitKoehler.class);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));      
		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		/* Konfiguration für die einzelnen Anwendungsfälle */
		/* conf: Task1 */
		if (useCase.equals("wc")) {
			System.out.println("wc: WordCount");	
			conf.setJobName("WordCount");
			/* Output: Key:Text -> Value:Integer */
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);
			conf.setOutputFormat(TextOutputFormat.class);
			/* Input: Key.Text -> Value:Text */
			conf.setInputFormat(TextInputFormat.class);
			/* Definition der entsprechenden Mapper/Reducer-Klassen */
			conf.setMapperClass(WordCountMap.class);
			conf.setCombinerClass(WordCountReduce.class);
			conf.setReducerClass(WordCountReduce.class);
		}
		/* conf: Task2 */
		else if (useCase.equals("cc_p")) {
			System.out.println("cc_p Berechnung v. Kookkurrenz mit Pairs-Algorithmus");	
			conf.setJobName("Kookkurrenz");
			/* Output: Key:Text -> Value:Integer */
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(TextOutputFormat.class);
			/* Input: Key.Text -> Value:Text */
			conf.setInputFormat(TextInputFormat.class);
			/* Definition der entsprechenden Mapper/Reducer-Klassen */
			conf.setMapperClass(KookkurrenzMitPairsMap.class);
			conf.setCombinerClass(KookkurrenzMitPairsReduce.class);
			conf.setReducerClass(KookkurrenzMitPairsReduce.class);
			//conf.setInputFormat(NonSplittableTextInputFormat.class);
		}
		/* conf: Task3 */
		else if (useCase.equals("cc_s")) {
			System.out.println("cc_p Berechnung v. Kookkurrenz mit Stripes-Algorithmus");	
			conf.setJobName("Kookkurrenz");
			/* Output: Key:Text -> Value:Text */
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(TextOutputFormat.class);
			/* Input: Key.Text -> Value:Text */
			conf.setInputFormat(TextInputFormat.class);
			/* Map-Output: Text -> MapWritable */
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(MapWritable.class);
			/* Definition der entsprechenden Mapper/Reducer-Klassen */
			conf.setMapperClass(KookkurrenzMitStripesMap.class);
			/* TODO: Erklären, wieso Stripes ohne Combiner funktioniert */
			conf.setReducerClass(KookkurrenzMitStripesReduce.class);
			//conf.setInputFormat(NonSplittableTextInputFormat.class);
		}
		/* default-option: Exit */
		else {
			printUsage();
			return 1;
		}
  
		/* Job starten */
		JobClient.runJob(conf);
		return 0;
	}
	/* </run> */
	 
	/*
	 * <Main>
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ProjektArbeitKoehler(), args);
		System.exit(res);
	}
	/* </Main> */
	
	/* 
	* <Helper-Klassen>
	*/
	private void printUsage() {
		System.out.println("usage: [usecase] [input-path] [output-path]");
		System.out.println("supported [usecase]:");
		System.out.println("     wc: WordCount");
		System.out.println("     cc_p: Kookkurrenz mit Pairs-Algorithmus");
		System.out.println("     cc_s: Kookkurrenz mit Stripes-Algorithmus");
		return;
	}
	
	private void deleteOldOutput(String outputPath) throws IOException {
		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);
	}
	/* </Helper-Klassen>	*/
}
