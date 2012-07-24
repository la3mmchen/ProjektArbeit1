package org.myorg;

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
/*
 * Durchgeführte Implementierungsleistung im Rahmen der Projektarbeit "Software Bibliotheke für das Information Retrieval: Hadoop - eine Einführung und praktische Beispiele zur Anwedung" 
 * von Alexander Köhler im Rahmen des VAWi-Studiums.
 * 
 */

public class ProjektArbeitKoehler extends Configured implements Tool {
	 /* <generics>
	  * Die Implementierung der Klasse WholeFileInputFormat und WholeFileRecordReader sind komplett übernommen aus dem Buch: "Hadoop: The Definitive Guide" -> Siehe Literaturverzeichnis am Ende der Projektarbeit
	  */
	public static class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

		@Override
		protected boolean isSplitable(FileSystem fs, Path filename) {
			return false;
		}

		@Override
		public RecordReader<NullWritable, BytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
			return new WholeFileRecordReader((FileSplit) split, job);	
		}
	}
	public static class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {

		private FileSplit fileSplit;
		private Configuration conf;
		private boolean processed = false;

		public WholeFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
			this.fileSplit = fileSplit;
			this.conf = conf;
		}

		@Override
		public NullWritable createKey() {
			return NullWritable.get();
		}

		@Override
		public BytesWritable createValue() {
			return new BytesWritable();
		}

		@Override
		public long getPos() throws IOException {
			return processed ? fileSplit.getLength() : 0;
		}

		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public boolean next(NullWritable key, BytesWritable value) throws IOException {
			if (!processed) {
				  byte[] contents = new byte[(int) fileSplit.getLength()];
				  Path file = fileSplit.getPath();
				  FileSystem fs = file.getFileSystem(conf);
				  FSDataInputStream in = null;
				  try {
					in = fs.open(file);
					IOUtils.readFully(in, contents, 0, contents.length);
					value.set(contents, 0, contents.length);
				  } finally {
					IOUtils.closeStream(in);
				  }
				  processed = true;
				  return true;
			}
			return false;
		}

		@Override
		public void close() throws IOException {
			// do nothing
		}
	}
	  /* </generics> */
	 
	 
	/* <Task1>: 
	 *  WordCount (wc): Zählt Vorkommenshäufigkeiten in der gegebenen Kollektion
	 * */
	public static class WordCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	 private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();
	  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString(); // Umwandlung InputSplit von Text nach String
		StringTokenizer tokenizer = new StringTokenizer(line); // Aufteilung anhand von Leerzeichen
		while (tokenizer.hasMoreTokens()) { // Iteration über die einzelnen Bestandteile des InputSplits
		  word.set(tokenizer.nextToken());
		  output.collect(word, one); // Erzeugen eines Eintrags im Zwischenergebnisses
		}
	  }
	}
	public static class WordCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0; // Summe
		while (values.hasNext()) {
		  sum += values.next().get(); // Aufsummierung der ermittelten Werte
		}
		output.collect(key, new IntWritable(sum)); // Schreiben des finalen Ergebnisses pro Term
	  }
	}
	/* </Task1> */
	 
	 /* <Task2>
	  * Kookkurrenz mit Pairs (cc_p): Kookkurrenz-Analyse nach dem Pairs-Design-Pattern
	  */
	public static class KookkurrenzMitPairsMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final Text pair = new Text();
		private final IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text line, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String text = line.toString();

			String[] terms = text.split("\\s+"); // Spliten bei Leerzeichen
			java.util.Arrays.sort(terms); // Alphabetisches Sortieren der ermittelten Terme

			for (int i = 0; i < terms.length-1; i++) { // Iteration über jeden gefundenen Term
				String term = terms[i];
				term = term.trim(); // Abschneiden v. führenden od. nachfolgenden Leerzeichen
						
				if (term.length() ==0  | term.equals("")) // Überspringen von leeren Termen
					continue;
					
				for (int j = i+1; j < terms.length; j++) {	// Iteration über aller Nachfolger
					String term2 = terms[j];
					term2 = term2.trim(); // Abschneiden v. führenden od. nachfolgenden Leerzeichen
					
					if (term2.length() == 0 | term2.equals("") ) // Überspringen von leeren Termen
						continue;
						
					boolean found = true; // Schalter für ein neues gefundenes Pärchen
					int compare = term.compareTo(term2); // Vergleich von Term mit Nachfolger  
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
					
					if (found) { // Nur eine Intermediate-K/V Pärchen erzeugen, wenn term & term2 unterschiedlich sind
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
				sum += values.next().get(); // Aufsummierung der ermittelten Werte
			}
			output.collect(key, new IntWritable(sum)); // Schreiben des finalen Ergebnisses pro Term
		}
	}
	/*</Task2> */

	 /* <Task3>
	  * Kookkurrenz mit Stripes (cc_s): Kookkurrenz-Analyse nach dem Stripes-Design-Pattern
	  */
	public static class KookkurrenzMitStripesMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, MapWritable> {
		private final Text key = new Text();
		private final IntWritable one = new IntWritable(1);
		/* 
		 * Notwendiges Format für Stripes-Algorithmus
		 * term -> %hm{"Wort1"->1; "Wort2"->4, ...} */
		public void map(LongWritable key, Text line, OutputCollector<Text, MapWritable> output, Reporter reporter) throws IOException {
			String text = line.toString();
			String[] terms = text.split("\\s+"); // Spliten bei Leerzeichen			 
			
			// HashMap als Hilfe zum Aufsummieren d. Werte d. Kookkurrenzmatrix
			HashMap<Text, IntWritable> hm = new HashMap<Text, IntWritable>();


			for (int i = 0; i < terms.length; i++) { // Iteration über jeden gefundenen Term
				Text term = new Text(terms[i].trim());  // Format term: Text 
				
				if (term.getLength() == 0)  // Überspringen von leeren Termen
					continue;
															
				for (int j = 0; j < terms.length; j++) {	// innere, geschachtelte Schleife über alle Terme
					Text term2 = new Text(terms[j].trim());
					
					if(term2.getLength() == 0) 
						continue;
					
					if (i == j)  // Skip wenn Iteration an der selben Stelle steht:  ein Termvorkommen nur einmal zählen
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
				output.collect(term,stripe); // Schreibe der Zeile d. Kookkurrenzmatrix in OutputCollector
			}
		}
	}	  
	public static class KookkurrenzMitStripesReduce extends MapReduceBase implements Reducer<Text, MapWritable, Text, Text> {
		public void reduce(Text key, Iterator<MapWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			int sum = 0;
			HashMap<Text, Integer> hm = new HashMap<Text, Integer>(); // HashMap zum zählen erstellen
		
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
								// Datenformat an dieser Stelle: 
								// 		ipsum	; dolor->1; lorem->1; amet->1; lorem->1
								// 		lorem	; amet->1; ipsum->1; dolor->1; ipsum->1
						}
					}
			}
			output.collect(key, new Text(hm.toString()));			
		}
	}	
	/*</Task3> */

	/* <Task4>
	 * Korrelationsanalyse mit Hilfe v. Pairs-Kookkurrenz-Algorithmus (cor_p)
	 * 
	 * Notwendiges Formate der Verzeichnisse
	 * Startverzeichnis
	 * - tags
	 * -- <...> // Zahlenwert
	 * --- tags<...>.txt
	 * - ht_descriptors/
	 * - eh_descriptors/
	 */
	public static class KorrelationsAnalysePairsMap extends MapReduceBase implements Mapper<NullWritable, BytesWritable, Text, IntWritable> {
		private final Text pair = new Text();
		private final IntWritable one = new IntWritable(1);
		private final String analysePath = "ht_descriptors"; // zu analysiereder Feature-Vektor
		private final String analyseFilePrefix = "ht"; // Prefix für Datei --> muss inhaltlich mit analysePath zusammen passen
		private final String kollektion = "tags";
		private int nGram = 3; // Definition der nGram-Breite; Optimierung: Könnte über Kommandozeilenargumente abgefragt werden
		public void map(NullWritable key, BytesWritable line, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			// für Zugriff den Feature-Vektor ist der Datei-Name der gerade durch den Mapper verarbeiteten Datei notwendig
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String filePathParent = fileSplit.getPath().getParent().toString();	
			String fileName = fileSplit.getPath().getName();	
		
			// auf Basis des Pfads der gerade verarbeiteten Datei die zugehörige Feature-Vektor Datei bestimmen
			String[] pathComponents = filePathParent.split("/");
			int pathInt = Integer.parseInt(pathComponents[pathComponents.length-1]);
			String htPath = pathComponents[pathComponents.length-3]+"/"+analysePath+"/";
			String ehFile = analyseFilePrefix+String.valueOf(pathInt)+".txt";
			
			// Zeilennummer aus Dateinamen extrahieren
			// Pattern 
			Pattern p = Pattern.compile(kollektion+"("+String.valueOf(pathInt-1)+")(\\d+)\\.txt");

			Matcher m = p.matcher(fileName);
			int neededLineNumber = 0;
			if (m.find()) {
				neededLineNumber = Integer.parseInt(m.group(2));
			}

			// Zugriff auf Datei mit Feature-Verktor mit dem Feature-Vektor
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(htPath+ehFile));
			String featureVektor = new String("");
	
			if (neededLineNumber != 0 ) { // Map nur fortsetzen, wenn eine Zeilennummer aus dem Dateinamen ermittelt werden konnte
				try {
						BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
						featureVektor=br.readLine();
						
						for(int o=1; o<neededLineNumber-1; o++) { // Datei bis neededLineNumber - 1 lesen um LineCounter an die korrekte Stelle zu setzen
							featureVektor=br.readLine();					
						}
						featureVektor=br.readLine(); // featureVektor enthält nun den Feature-Vektor zur eingelesenen Datei
				}
				catch(Exception e){
							System.out.println("File not found");
					}
					
				if (featureVektor.length() != 0 | !featureVektor.equals("")) { // nur fortfahren, wenn anhand d. Zeilennummer ein Feature-Vektor gefunden werden konnte
		
					// Verarbeiten des Feature-Vektor					
					String[] features = featureVektor.split("\\s+");
					List<String> nGramms = new ArrayList<String>();

					for (int i = 0; i < features.length-1; i++) { // Iteration über jedes gefundene Feature
						String current = features[i];
						current = quantify(current);
						
						if ((i + nGram) > features.length) // Abbrechen, wenn aufgrund der nGram Größe keine weitere nGrams mehr gebildet werden können
							break;
						
						for (int x = 1; x < nGram; x++) {
							current = current + " " + quantify(features[i+x]);
						}
						nGramms.add(current);
					}
					
					// Verarbeitung der Tags sowie Kombination mit ermittelten nGramms
					
					// Umwandeln der Bytes-Folge aus BytesWritable in einen String
					String bytes2stringHelper = new String(line.getBytes()); 
					String[] tags = bytes2stringHelper.split("\\r?\\n"); // Spliten bei CR oder LF
										
					for (int i = 0; i < tags.length-1; i++) { // Iteration über jedes gefundene Feature
						String tag = tags[i];
						tag = tag.trim();
						Iterator itr = nGramms.iterator();
						
						while(itr.hasNext()) {
								output.collect(new Text(itr.next()+" -> "+tag), one); // add n-Gramm plus tag zu Output
						}
					}
				}
			} 
		}	
		private static String quantify(String str) {
			return Double.toString(Math.rint( Double.parseDouble(str) * 100 ) / 100 );
		}
	}
	public static class KorrelationsAnalysePairsReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get(); // Aufsummierung der ermittelten Werte
			}
			output.collect(key, new IntWritable(sum)); // Schreiben des finalen Ergebnisses pro Term
		}
	}
	/*</Task4> */ 


	/* <Task5>
	 * Korrelationsanalyse Tags zu EXIF-Daten mit Hilfe v. Pairs-Kookkurrenz-Algorithmus (cor_exif)
	 */
	public static class KorrelationsAnalyseExifPairsMap extends MapReduceBase implements Mapper<NullWritable, BytesWritable, Text, IntWritable> {
		private final Text pair = new Text();
		private final IntWritable one = new IntWritable(1);
		private int nGram = 3; // Definition v. n; Optimierung: Könnte über Kommandozeilenargumente abgefragt werden
		public void map(NullWritable key, BytesWritable line, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			// Umwandeln der Bytes-Folge aus BytesWritable in einen String
			String bytes2stringHelper = new String(line.getBytes()); 
			String[] exifParts = bytes2stringHelper.split("\\r?\\n"); // Spliten bei CR oder LF
			List<String> exifs = new ArrayList<String>();
			
			for (int i = 0; i < exifParts.length; i++) { // Iteration über jedes gefundene Zeile
						String actLine = exifParts[i];
						actLine = actLine.trim();
						if (actLine.equals(""))
							continue;
						if(actLine.substring(0,1).equals("-"))  {
							if (i+1 < exifParts.length) {
								String nextLine = exifParts[i+1];
								if (!nextLine.equals("")) {
										exifs.add(actLine+":"+nextLine);
									}
							}
						}
			}
			
			// Zur Kombination der Exif-Daten mit den Tags wird die Tag-Datei benötigt
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String fileName = fileSplit.getPath().getName();	
			String filePathParent = fileSplit.getPath().getParent().toString();
			filePathParent = filePathParent+"/"+fileName;
			filePathParent = filePathParent.replace("exif", "tags");
			
			List<String> tags = new ArrayList<String>();
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(filePathParent));
			try {
					String currentLine;
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
					while((currentLine = br.readLine()) != null) {
							tags.add(currentLine);
					}
			}
			catch(Exception e){
						System.out.println("File not found");
				}

					
			for(int x=0; x<exifs.size();x++) {
					String actExif = exifs.get(x);
					for(int i=0; i<tags.size(); i++) {
							String actTag = tags.get(i);
							output.collect(new Text(actExif + " " + actTag), one);	
					}				
			}
		}	
	}
	public static class KorrelationsAnalyseExifPairsReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if (sum >=3)
				output.collect(key, new IntWritable(sum));
		}
	}
	/*</Task5> */ 

	/* <run>
	 * 
	 * @param args Kommandozeilenparameter; Syntax: (String)Anwendungsfalls (String)Input-Dir (String)Output-Dir
	 **/
	public int run(String[] args) throws Exception {
		//Prüfen ob geforderte Mindestanzahl an Parametern übergeben wurde
		if (args.length != 3) {
			printUsage();
			return 1;
		}
		// Werte aus args aufbereiten
		String useCase = args[0];
		String inputPath = args[1];
		String outputPath = args[2];
	
		// Löschen eines vorhandenen "outputPath" Verzeichnis
		deleteOldOutput(outputPath);		
		
		// globale Konfiguration
		JobConf conf = new JobConf(ProjektArbeitKoehler.class);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));      
		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		// Konfiguration für die einzelnen Anwendungsfälle 
		// conf: Task1 
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
		// conf: Task2
		else if (useCase.equals("cc_p")) {
			System.out.println("cc_p Berechnung v. Kookkurrenz mit Pairs-Algorithmus");	
			conf.setJobName("Kookkurrenz");
			/* Output: Key:Text -> Value:Integer */
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(TextOutputFormat.class);
			/* Input: Key.Text -> Value:Text */
			conf.setInputFormat(TextInputFormat.class); 
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(IntWritable.class);
			/* Definition der entsprechenden Mapper/Reducer-Klassen */
			conf.setMapperClass(KookkurrenzMitPairsMap.class);
			conf.setCombinerClass(KookkurrenzMitPairsReduce.class);
			conf.setReducerClass(KookkurrenzMitPairsReduce.class);
			
		}
		// conf: Task3
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
			conf.setReducerClass(KookkurrenzMitStripesReduce.class);
			//conf.setInputFormat(NonSplittableTextInputFormat.class);
		}
		// conf: Taks 4
		else if (useCase.equals("cor_p")) {
			System.out.println("cor_p Korrelationsanalyse auf Basis des Pairs-Algorithmus");	
			conf.setJobName("Korrelationsanalyse");
			/* Output: Key:Text -> Value:Integer */
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);
			conf.setOutputFormat(TextOutputFormat.class);
			/* Input: Key.Text -> Value:Text */
			conf.setInputFormat(WholeFileInputFormat.class); // Korrelation benötigt ein Eingabeformat in dem eine ganze Datei an den Mapper geht
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(IntWritable.class);
			/* Definition der entsprechenden Mapper/Reducer-Klassen */
			conf.setMapperClass(KorrelationsAnalysePairsMap.class);
			conf.setCombinerClass(KorrelationsAnalysePairsReduce.class);
			conf.setReducerClass(KorrelationsAnalysePairsReduce.class);
			// Überschreiben des InputPaths um auch alle Subdirectories zu berücksichtigen
			FileInputFormat.setInputPaths(conf, new Path(inputPath+"tags/*")); // setInputPaths = tags/*
		}
		// conf: Task5
		else if (useCase.equals("cor_exif")) {
			System.out.println("cor_exif: Korrelationsanalye v. Exif-Daten und Tags auf Basis v. Pairs");	
			conf.setJobName("Korrelationsanalyse");
			/* Output: Key:Text -> Value:Integer */
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(TextOutputFormat.class);
			/* Input: Key.Text -> Value:Text */
			conf.setInputFormat(WholeFileInputFormat.class); 
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(IntWritable.class);
			/* Definition der entsprechenden Mapper/Reducer-Klassen */
			conf.setMapperClass(KorrelationsAnalyseExifPairsMap.class);
			conf.setCombinerClass(KorrelationsAnalyseExifPairsReduce.class);
			conf.setReducerClass(KorrelationsAnalyseExifPairsReduce.class);
			
		}
		// default-option: Exit
		else {
			printUsage();
			return 1;
		}
  
		// Job starten
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
	
	/* <Helper-Klassen>
	*/
	private void printUsage() {
		System.out.println("usage: [usecase] [input-path] [output-path]");
		System.out.println("supported [usecase]:");
		System.out.println("     wc: WordCount");
		System.out.println("     cc_p: Kookkurrenz mit Pairs-Algorithmus");
		System.out.println("     cc_s: Kookkurrenz mit Stripes-Algorithmus");
		System.out.println("     cor_p: Korrelationsanalye mit dem Pairs-Algorithmus");
		System.out.println("     cor_exif: Korrelationsanalye v. Exif-Daten und Tags auf Basis v. Pairs");
		return;
	}
	
	private void deleteOldOutput(String outputPath) throws IOException {
		// Output Verzeichnis löschen, wenn bereits vorhanden
		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);
	}
	/* </Helper-Klassen>	*/
}
