import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

//poi-4.0.1.jar
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook; 
import org.apache.poi.ss.usermodel.FillPatternType;
//poi-ooxml-4.0.1.jar
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//jar dependency: xmlbeans-3.0.2.jar, commons-collections4-4.2.jar, commons-compress-1.18.jar, poi-ooxml-schemas-4.0.1.jar


import org.apache.spark.util.LongAccumulator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import scala.Tuple2;


public class MainClass {

	// variabili globali
	static String comando = "comando_di_lancio";
	static List<Integer> labels = new ArrayList<>(); 		// lunghezza = numero elementi nel file. Ogni elemento e' un intero che indica la classe
	static List<Integer> occorrenze = new ArrayList<>(); 	// lunghezza = classi+1. Ogni elemento e' un intero che indica il numero di occorrenze della classe nel file. L'elemento 0 indica le occorrenze totali
	static int classi; 										// numero di classi
	static int coordinate; 									// numero di coordinate
	static double[][] W = new double[0][0]; 				// Matrice dei pesi. Serve per preparare i pesi di partenza per il BVQ
	static int[] L = new int[0]; 							// vettore labels

	// Varibiali globali per leggere da file
	static FileInputStream file_comando;
	static FileInputStream file_dati;
	static BufferedReader buffer;

	// variabili utilizzate per excel
	static FileOutputStream fileOut;
	static Workbook wb;
	static Sheet sheet1, sheet2;
	static int indice_riga_excel=0;
	
	// variabile per la scrittura in output csv
	static FileWriter fw;
	static String[][] pesi_prima_som, pesi_prima_bvq, pesi_dopo_bvq;
	static int indice_csv=0;

	// dati che prendo da file
	static String n_vcod_classe_str; 	// numero di vettori codice per classe in formato stringa
	static String data; 			//datiNormBVQ
	static String dir; 				//directory di output (results)
	static int epoche; 				// numero di epoche BVQ
	static int epocheSOM; 			// numero di epoche di SOM		
	static double delta; 			// distanza dal bordo di decisione
	static double lr; 				// learning rate
	static boolean flag_iteraz; 	// False: considera le epoche, True: considera le iterazioni
	static boolean flag_SOM; 		// True: SOM
	static String costoStr; 		// matrice di costo presa come stringa e poi convertita in matrice
	static int percentuale_train;	// percentuale di dati da prendere per il train
	static long seed;
	static int dim_bag;				// dimensione di partizioni da fare
	static int n_bag;				// numero di partizioni (numero di iterazioni di BVQ)
	static String replicabile;		// permette la replicabilita' del test
	static boolean excel;			// true se vogliamo i risultati in formato excel

	// matrice dei costi:
	// static double[][] costo;
	// costo=[0 1; 1 0];
	// costo=[0 300/(300+700); 700/(300+700) 0]; //german
	// costo=[0 268/(268+500); 500/(268+500) 0]; //diabetes
	// costo=[0 239/(239+444); 444/(239+444) 0]; //breast_w
	// costo=[0 225/(225+126); 126/(225+126) 0]; //ionosphere
	// costo=[0 200/(200+145); 145/(200+145) 0]; //liver
	// costo=[0 137/(137+160); 160/(137+160) 0]; //heart
	// costo=[0 518/(518+1210); 1210/(518+1210) 0]; //car
	// costo=[0 383/(383+307); 307/(383+307) 0]; //australian
	// nuovi esperimenti
	// costo=[0 nc2/(nc2+nc1); nc1/(nc2+nc1) 0]; 
	// costo=[0 2870/(2870+293); 293/(2870+293) 0]; //euthyroid
	// costo=[0 3012/(3012+151); 151/(3012+151) 0]; //hypothyroid
	// costo=[0 123/(123+32); 32/(123+32) 0]; //hepatitis
	// costo=[0 150/(150+120); 120/(150+120) 0]; //heart_statalog
	static double[][] matCosto;
	
	static int[] n_vcod_classe; 		// numero di vettori codice per classe in formato intero
	
	// Variabili utilizzate per spark
	static SparkSession spark_session;
	static LongAccumulator accum;
	static Dataset<org.apache.spark.sql.Row> data_spark; 
	static Dataset<org.apache.spark.sql.Row> filt;
	static JavaSparkContext sc;
	static List<Long> totalTime_SOM_list = new ArrayList<>();
	
	public static void main(String args[])
			throws IOException {

		// faccio partire il timer
		long totalTime_SOM = 0;
		long startTime = System.nanoTime();
		// **************************** NEL CASO SI UTILIZZI ECLIPSE ********************************************
		// *************** Aggiungere qualcosa come argomento nel run configuration *****************************
		if(args.length != 0){

			String resource_path = "src/main/resources/";
			
			// file che contiene il comando di lancio
			file_comando = new FileInputStream(resource_path + "dati_input/" + comando + ".txt");

			// buffer che mi consente di estrapolare le varie righe
			buffer=new BufferedReader(new InputStreamReader(file_comando));

			// Estrapolo i dati dal file comando_di_lancio.txt
			comando_input(buffer, file_comando);

			// Chiudo il comando di lancio e il buffer
			buffer.close();
			file_comando.close();

			// riempio il buffer con i dati per train e test
			file_dati = new FileInputStream(resource_path + "dati_input/" + data +".csv");
			buffer=new BufferedReader(new InputStreamReader(file_dati,"UTF-8"));
						
			// **************************** EXCEL ********************************************
			if(excel) {
				// Creo un WorkBook in formato xlsx
				wb = new XSSFWorkbook();

				// Creo un foglio excel
				sheet1 = wb.createSheet("Risultati");
				sheet2 = wb.createSheet("Vettori Codice");
				// File per la scrittura excel eclipse
				fileOut = new FileOutputStream(resource_path + "dati_output/"+dir+".xlsx");
			}
			// Se invece non vogliamo l'excel, elimino eventuali residui
			else if(new File(resource_path + "dati_output/"+dir+".xlsx").exists()) new File(resource_path + "dati_output/"+dir+".xlsx").delete();
			// *******************************************************************************

			
			// file per la scrittura csv eclipse
			File file_out = new File(resource_path + "dati_output/"+dir+".csv");
			fw = new FileWriter(file_out.getAbsolutePath());
			System.out.println(file_out);	
			// Spark
			String path = resource_path + "dati_input/"+ data +".csv"; // sample_multiclass_classification_data.     classe #coordinata:coordinata ...

			// Creo una sessione spark che è l'evoluzione dello spark context. Questo ci permetterà poi di usare le funzioni sql di spark e di lavorare con i DataFrame che sono molto più efficienti degli RDD

			SparkSession spark_session = SparkSession
					  .builder()
					  .appName("BVQ Spark")
					  .master("local")
					  .getOrCreate();
			
			sc = new JavaSparkContext(spark_session.sparkContext());
			
			// decido di stampare soltanto gli errori e non le INFO
			sc.setLogLevel("ERROR");
			
			// Creo lo schema dei dati che importo
			
			StructType schema = new StructType()
				.add("id", "int")
			    .add("label", "double")
			    .add("feature1", "double")
			    .add("feature2", "double");
			
			// Carico di dati dal csv in un DataFrame 
			
			data_spark = spark_session.read()
					.schema(schema)
					.option("partitionColumn", "label")
					.csv(path).cache();
			// ******************************************************************************************************
		}
		else{
			// *************************** NEL CASO SI UTILIZZI IL JAR **********************************************

			// Prendo la directory del jar
			File jarDir = new File(ClassLoader.getSystemClassLoader().getResource(".").getPath());

			//file
			file_comando = new FileInputStream(jarDir.getAbsolutePath()+"/dati_input/" + comando + ".txt");

			// buffer che mi consente di estrapolare le varie righe
			buffer=new BufferedReader(new InputStreamReader(file_comando));

			// Estrapolo i dati dal file comando_di_lancio.txt
			comando_input(buffer, file_comando);

			// Chiudo il comando di lancio e il buffer
			buffer.close();
			file_comando.close();

			// riempio il buffer con i dati per train e test
			file_dati = new FileInputStream(jarDir.getAbsolutePath()+"/dati_input/" + data +".csv");
			buffer=new BufferedReader(new InputStreamReader(file_dati,"UTF-8"));
			
			// **************************** EXCEL ********************************************
			if(excel) {
				// Creo un WorkBook in formato xlsx
				wb = new XSSFWorkbook();

				// Creo un foglio excel
				sheet1 = wb.createSheet("Risultati");
				sheet2 = wb.createSheet("Vettori Codice");
				// File per la scrittura excel jar
				fileOut = new FileOutputStream(jarDir.getAbsolutePath()+"/dati_output/"+dir+".xlsx");
			}
			// Se invece non vogliamo l'excel, elimino eventuali residui
			else if(new File(jarDir.getAbsolutePath()+"/dati_output/"+dir+".xlsx").exists()) new File(jarDir.getAbsolutePath()+"/dati_output/"+dir+".xlsx").delete();
			// *******************************************************************************
						
			// file per la scrittura csv jar
			fw = new FileWriter(jarDir.getAbsolutePath()+"/dati_output/"+dir+".csv");
						
			// Spark
			String path = jarDir.getAbsolutePath()+"/dati_input/" + data +".csv";
			
			// Creo una sessione spark che è l'evoluzione dello spark context. Questo ci permetterà poi di usare le funzioni sql di spark e di lavorare con i DataFrame che sono molto più efficienti degli RDD
			SparkSession spark_session = SparkSession
					  .builder()
					  .appName("BVQ Spark")
					  .master("local")
					  .getOrCreate();
			
			sc = new JavaSparkContext(spark_session.sparkContext());
			
			// decido di stampare soltanto gli errori e non le INFO
			sc.setLogLevel("ERROR");
						
			// Creo lo schema dei dati che importo
			
			StructType schema = new StructType()
				.add("id", "int")
			    .add("label", "double")
			    .add("feature1", "double")
			    .add("feature2", "double");
			
			// Carico di dati dal csv in un DataFrame 
			
			data_spark = spark_session.read()
					.schema(schema)
					.option("partitionColumn", "label")
					.csv(path).cache();
			// ******************************************************************************************************
		}
		classi = conta_classi(buffer, file_dati);

		coordinate = conta_coordinate(buffer, file_dati);

		conta_righe(classi);

		// Chiudo lo stream, il file dei dati e svuoto lista
		buffer.close();
		file_dati.close();
		labels.clear();

		costToMat(); // Trasformo la matrice dei costi da String a double[][]
		
		VcodToVec(); // Trasformo il numero di vettori codice per classe da String a int[]
		
		int n_vcod = IntStream.of(n_vcod_classe).sum(); // Totale vettori codice
		
		// Preparo variabili per la scrittura finale nel csv
		pesi_prima_som = new String[n_vcod][coordinate+1];
		pesi_prima_bvq = new String[n_vcod][coordinate+1];
		pesi_dopo_bvq = new String[n_vcod][coordinate+1];
		// fine			

		System.out.println("\n************************************** Inizio Splitting dati *************************************");

		
		// ************************************************* SPARK ************************************************************
		double perc = ((double)percentuale_train)/100;
		// Dividiamo in due i Dataset... [perc% training data, (1-perc)% testing data]. seed è il seme
		
		Dataset<org.apache.spark.sql.Row>[] splits = data_spark.randomSplit(new double[]{perc, 1-perc}, seed);
		Dataset<org.apache.spark.sql.Row> training_spark = splits[0];
		JavaRDD<org.apache.spark.sql.Row> test_spark= splits[1].toJavaRDD();
		training_spark.cache();
		test_spark.cache();
		long dim_pset = training_spark.count(); // Serve per avere  una sola variabile che conta le occorrenze del training_set
		
		// ********************************************************************************************************************
		
		// Se flag_SOM==True nel file di comando, allora...
		if(flag_SOM){ 
			
			for( int i=0;i<classi;i++){
				eseguiSOM(i+1, training_spark);
				
				totalTime_SOM = totalTime_SOM + totalTime_SOM_list.get(i);
			}
			
			// **************** Libero memoria ***************
			System.gc();
			// ***********************************************
			
		}else{
			
			// ************************** Creazione Pesi/Train/Test su intero blocco *****************************
			  for( int i=0;i<classi;i++) eseguiIniz(i+1 , training_spark);
			// ***************************************************************************************************
		}
		
		// Chiamo costruttore della classe Net
		Net rete = new Net(W, L, matCosto, epoche, n_bag, lr, delta);

		// ******************** creo una matrice che e' una composizione di W e L, in modo da poterla ordinare *********************
		double[][] temp_matrix = new double[W.length][W[0].length+1];
		for(int i=0;i<rete.getweight().length;i++)
		{
			for(int j=0;j<rete.getweight()[0].length;j++)
			{
				temp_matrix[i][j]=rete.getweight()[i][j];
			}
			temp_matrix[i][rete.getweight()[0].length]=L[i];
		}
		Funx.selectionSort(temp_matrix);
		
		for(int i=0;i<temp_matrix.length;i++) for(int j=0;j<temp_matrix[0].length;j++) pesi_prima_bvq[i][j] = temp_matrix[i][j]+"";
		// *************************************************************************************************************************

		System.out.println("\n*************************************** Inizio Training BVQ **************************************");

		// salvo il tempo di conclusione dell'app
		long startTime_trainBVQ= System.nanoTime();
		
		rete.train(training_spark, flag_iteraz, seed, n_bag, dim_bag, replicabile);
		
		// salvo il tempo di conclusione dell'app
		long endTime_trainBVQ= System.nanoTime();
		System.out.println("\n*************************************** Inizio Testing BVQ ***************************************");

		// Compute raw scores on the test set.
		JavaPairRDD<Object, Object> predictionAndLabels = test_spark.mapToPair(r -> new Tuple2<>(
				rete.valuta2(featureToDoubleVec(r),r.getDouble(1)), 
				r.get(1)));
		
		predictionAndLabels.cache();

		// Get evaluation metrics.
		MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

		System.out.println("\n*********************************** Calcolo Matrice Confusione ***********************************");

		// Confusion matrix
		Matrix confusion = metrics.confusionMatrix();
		System.out.println("Matrice di confusione: \n" + confusion);

		System.out.println("\n*************************************** Calcolo accuratezza **************************************");

		// Overall statistics
		System.out.println("Accuratezza = " + metrics.accuracy());
		System.out.println("\n************************************** Calcolo altre metriche ************************************");

		// Stats by labels
		for (int i = 0; i < metrics.labels().length; i++) {
			System.out.format("Classe %f precision = %f\n", metrics.labels()[i],metrics.precision(
					metrics.labels()[i]));
			System.out.format("Classe %f recall = %f\n", metrics.labels()[i], metrics.recall(
					metrics.labels()[i]));
			System.out.format("Classe %f F1 score = %f\n", metrics.labels()[i], metrics.fMeasure(
					metrics.labels()[i]));
		}

		//Weighted stats
		System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());
		System.out.format("Weighted recall = %f\n", metrics.weightedRecall());
		System.out.format("Weighted F1 score = %f\n", metrics.weightedFMeasure());
		System.out.format("Weighted false positive rate = %f\n", metrics.weightedFalsePositiveRate());
		// *************************************************************************************************************************

		
		// ******* riordino la matrice dei pesi, salvandola precedentemente su una variabile locale in modo da unire W con L *******
		for(int i=0;i<rete.getweight().length;i++)
		{
			for(int j=0;j<rete.getweight()[0].length;j++)
			{
				temp_matrix[i][j]=rete.getweight()[i][j];
			}
			temp_matrix[i][rete.getweight()[0].length]=L[i];
		}
		Funx.selectionSort(temp_matrix);

		for(int i=0;i<temp_matrix.length;i++) for(int j=0;j<temp_matrix[0].length;j++) pesi_dopo_bvq[i][j] = temp_matrix[i][j]+"";
		// *************************************************************************************************************************
		
		
		// salvo il tempo di fine
		long endTime   = System.nanoTime();
		
		// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ SCRITTURA EXCEL @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		if(excel) {
			System.out.println("\n*********************************** Scrittura excel: Risultati ***********************************");
			scrivo_cella(sheet1,0,0,"RISULTATI", IndexedColors.CORAL.getIndex());

			scrivo_cella(sheet1,2,0,"Replicabile:");
			scrivo_cella(sheet1,2,1, replicabile);
			
			scrivo_cella(sheet1,4,0,"Numero di vettore codice:");
			scrivo_cella(sheet1,4,1,n_vcod);
			scrivo_cella(sheet1,5,0,"Matrice dei costi:");
			scrivo_cella(sheet1,5,1,"["+costoStr+"]");

			int riga=7;
			if(flag_SOM) {
				scrivo_cella(sheet1,riga,0,"Inizializzazione tramite SOM:", IndexedColors.BLUE_GREY.getIndex());
				scrivo_cella(sheet1,riga+1,0,"Numero epoche:");
				scrivo_cella(sheet1,riga+1,1,epocheSOM);
				riga=riga+3;
			}
			else
			{
				scrivo_cella(sheet1,riga,0,"Inizializzazione tramite randomPick", IndexedColors.BLUE_GREY.getIndex());
				riga=riga+2;
			}
			
			
			if(flag_iteraz)
			{	
				scrivo_cella(sheet1,riga,0,"BVQ con iterazioni:", IndexedColors.CORNFLOWER_BLUE .getIndex());
				scrivo_cella(sheet1,riga+1,0,"Numero bag:");
				scrivo_cella(sheet1,riga+1,1,n_bag);
				scrivo_cella(sheet1,riga+2,0,"Dimensione bag:");
				scrivo_cella(sheet1,riga+2,1,dim_bag);
				riga=riga+4;
			}
			else {
				scrivo_cella(sheet1,riga,0,"BVQ con epoche:", IndexedColors.CORNFLOWER_BLUE.getIndex());
				scrivo_cella(sheet1,riga+1,0,"Numero epoche:");
				scrivo_cella(sheet1,riga+1,1,epoche);
				riga=riga+3;
			}

			scrivo_cella(sheet1,riga,0,"Dimensione test:");
			scrivo_cella(sheet1,riga,1,test_spark.count());

			scrivo_cella(sheet1,riga+1,0,"Dimensione train:");
			scrivo_cella(sheet1,riga+1,1,dim_pset);
			riga=riga+3;
			
			scrivo_cella_tempo(sheet1,riga,0,endTime, startTime, "Tempo di esecuzione TOT:");
			riga++;
			if(flag_SOM) 
			{
				scrivo_cella_tempo(sheet1,riga,0,totalTime_SOM, 0, "Tempo di esecuzione SOM:");
				riga++;
			}
			scrivo_cella_tempo(sheet1,riga,0,endTime_trainBVQ, startTime_trainBVQ, "Tempo di esecuzione train BVQ:");
			riga=riga+2;
			
			// creo la matrice di confusione tra "classe vera" e "classe risultante DOPO il train con Spark"
			scrivo_cella(sheet1,riga,0,"Matrice di confusione dopo il train con Spark:", IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
			riga++;
			// stampo la matrice di confusione
			int elem=0;
			for(int i=0;i<classi;i++)
			{
				for(int j=0;j<classi;j++)
				{
					scrivo_cella(sheet1,riga+j,i,confusion.toArray()[elem]);
					elem++;
				}
			}	
			riga=riga+classi+1;
			
			scrivo_cella(sheet1,riga,0,"Accuratezza:", IndexedColors.LIGHT_TURQUOISE.getIndex());
			riga++;
			scrivo_cella(sheet1,riga,0,metrics.accuracy());
			riga++;

			System.out.println("\n********************************* Scrittura excel: Vettori Codice ********************************");

			if(flag_SOM) {
				scrivo_cella(sheet2,indice_riga_excel,0,"Pesi prima del SOM:",IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
				indice_riga_excel++;
				for(int i=0;i<pesi_prima_som.length;i++)
				{
					for(int j=0;j<pesi_prima_som[0].length;j++)
					{
						scrivo_cella(sheet2,indice_riga_excel,j,Double.valueOf(pesi_prima_som[i][j]));
					}
					indice_riga_excel++;
				}
				indice_riga_excel++;
			}

			scrivo_cella(sheet2,indice_riga_excel,0,"Pesi prima del train BVQ:",IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
			indice_riga_excel++;
			for(int i=0;i<pesi_prima_bvq.length;i++)
			{
				for(int j=0;j<pesi_prima_bvq[0].length;j++)
				{
					scrivo_cella(sheet2,indice_riga_excel,j,Double.valueOf(pesi_prima_bvq[i][j]));
				}
				indice_riga_excel++;
			}
			indice_riga_excel++;


			scrivo_cella(sheet2,indice_riga_excel,0,"Pesi dopo il train BVQ:",IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
			indice_riga_excel++;
			for(int i=0;i<pesi_dopo_bvq.length;i++)
			{
				for(int j=0;j<pesi_dopo_bvq[0].length;j++)
				{
					scrivo_cella(sheet2,indice_riga_excel,j,Double.valueOf(pesi_dopo_bvq[i][j]));
				}
				indice_riga_excel++;
			}
			indice_riga_excel++;
		}
		// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		
		// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ SCRITTURA CSV @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		System.out.println("\n************************************ Scrittura csv: Risultati ************************************\n");
		
		String s = costoStr;
		s="["+s.replaceAll(";", ",")+"]";
		scrivi("RISULTATI\n\nReplicabile:; "+replicabile+"\n\nNumero di vettore codice:; "+n_vcod+"\nMatrice dei costi:; "+s+"\n\n");
		
		if(flag_SOM) scrivi("Inizializzazione tramite SOM:\nNumero epoche:; "+epocheSOM+"\n\n");
		else scrivi("Inizializzazione tramite randomPick\n\n");
		if(flag_iteraz) scrivi("BVQ con iterazioni:\nNumero bag:; "+n_bag+"\nDimensione bag:; "+dim_bag+"\n\n");
		else scrivi("BVQ con epoche:\nNumero epoche:; "+epoche+"\n\n");
			
		scrivi("Dimensione test:; "+test_spark.count()+"\nDimensione train:; "+dim_pset+"\n\nTempo di esecuzione TOT:; "+((endTime-startTime)/1000000000)+" secondi"+"\n");
		if(flag_SOM) scrivi("Tempo di esecuzione SOM:; "+(totalTime_SOM/1000000000)+" secondi\n");
		scrivi("Tempo di esecuzione train BVQ:; "+((endTime_trainBVQ-startTime_trainBVQ)/1000000000)+" secondi"+"\n\n");
		
		scrivi("Matrice di confusione dopo il train con Spark:\n");
		int elem=0;
		for(int i=1;i<=classi;i++)
		{
			for(int j=0;j<classi;j++)
			{
				scrivi((int)(confusion.toArray()[elem])+";");
				elem=elem+classi;
			}
			scrivi("\n");
			elem=i;
		}
		
		scrivi("\nAccuratezza:\n");
		scrivi(metrics.accuracy()+"\n");
		
		if(flag_SOM) {
			scrivi("\nPesi prima del SOM:\n");
			for(int i=0;i<pesi_prima_som.length;i++)
			{
				for(int j=0;j<pesi_prima_som[0].length-1;j++)
				{
					scrivi(pesi_prima_som[i][j]);
					scrivi(";");
				}
				scrivi("\""+pesi_prima_som[i][pesi_prima_som[0].length-1]+"\""); //scrivo la label tra gli apici
				scrivi("\n");
			}
		}
		
		scrivi("\nPesi prima del train BVQ:\n");
		for(int i=0;i<pesi_prima_bvq.length;i++)
		{
			for(int j=0;j<pesi_prima_bvq[0].length-1;j++)
			{
				scrivi(pesi_prima_bvq[i][j]);
				scrivi(";");
			}
			String label_int = pesi_prima_bvq[i][pesi_prima_bvq[0].length-1].replaceAll(".0", ""); // tolgo il .0 sulla label
			scrivi("\""+label_int+"\"");
			scrivi("\n");
		}
		
		scrivi("\nPesi dopo il train BVQ:\n");
		for(int i=0;i<pesi_dopo_bvq.length;i++)
		{
			for(int j=0;j<pesi_dopo_bvq[0].length-1;j++)
			{
				scrivi(pesi_dopo_bvq[i][j]);
				scrivi(";");
			}
			String label_int = pesi_dopo_bvq[i][pesi_dopo_bvq[0].length-1].replaceAll(".0", ""); // tolgo il .0 sulla label
			scrivi("\""+label_int+"\"");
			scrivi("\n");
		}
		// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

		// Test per vedere se effettivamente funziona la SOM
		//TestSOM tests = new TestSOM(100, 2);
		//tests.run();
		
		scrivo_console_tempo(endTime, startTime, "Tempo di esecuzione totale - %02d:%02d:%02d:%02d");
		if(flag_SOM) scrivo_console_tempo(totalTime_SOM, 0, "Tempo di esecuzione della SOM - %02d:%02d:%02d:%02d");
		scrivo_console_tempo(endTime_trainBVQ, startTime_trainBVQ, "Tempo di esecuzione della train BVQ - %02d:%02d:%02d:%02d");
		
		
		
		// ***************************************************************************************************************
		// ***************************************************************************************************************
		// *********************************************** TERMINAZIONE **************************************************
		// ***************************************************************************************************************
		// ***************************************************************************************************************

		if(excel) {
		// Setto automaticamente la larghezza della colonna
		sheet1.autoSizeColumn(0);
		sheet1.autoSizeColumn(1);
		// Setto automaticamente la larghezza della colonna
		for(int i=0;i<temp_matrix[0].length;i++) sheet2.autoSizeColumn(i);
		// Chiudo lo stream excel
		wb.write(fileOut);
		wb.close();
		fileOut.close();
		}
		
		// chiudo lo stream del csv
		fw.flush();
		fw.close();

		// chiudo spark context dopo X secondi
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		sc.close();
		// ***************************************************************************************************************
		// ***************************************************************************************************************
		// ***************************************************************************************************************
		// ***************************************************************************************************************
		// ***************************************************************************************************************
	}

	// ********************************************************************************************************************************************************************
	// 		Funzione che trasforma la matrice dei costi da stringa a matrice
	// ********************************************************************************************************************************************************************
	public static void costToMat (){
		String[] riga, elem;
		matCosto = new double[classi][classi]; // creo la variabile matriceDiCosto

		costoStr = costoStr.replaceAll("[\\[\\]]", ""); // elimino le parentesi quadre e aperte
		riga=costoStr.split(";"); // separo in righe (in posizione 0 avro' la prima riga della matrice)
		for(int i=0; i<classi; i++) { //i-riga
			elem = riga[i].split(" "); //elem e' un vettore contentente in ogni posizione, un numero
			for(int j=0; j<classi; j++) //j-colonna
				matCosto[i][j] = Double.valueOf(elem[j]);
		}
	}

	// ********************************************************************************************************************************************************************
	// 		Funzione che trasforma il numero di vettori codice da stringa a vettore di interi
	// ********************************************************************************************************************************************************************
	public static void VcodToVec (){
		String[] elem;
		n_vcod_classe = new int[classi]; // creo la variabile vettore di vettori codice

		n_vcod_classe_str = n_vcod_classe_str.replaceAll("[\\[\\]]", ""); // elimino le parentesi quadre e aperte
		elem=n_vcod_classe_str.split(","); // separo in righe (in posizione 0 avro' il primo numero di vettori codice)
		for(int i=0; i<classi; i++) { //Scorro gli elementi e li assegno al vettore dei vettori codice
			n_vcod_classe[i] = Integer.valueOf(elem[i]);
		}
	}

	// ********************************************************************************************************************************************************************
	// 		Prende i dati di lancio
	// ********************************************************************************************************************************************************************
	public static void comando_input(BufferedReader buffer, FileInputStream file) throws IOException{
		String riga;

		String[][] input = new String[16][2];
		int i=0;
		riga = buffer.readLine();
		String[] temp = new String[2];
		while (riga != null){
			temp=riga.split(": "); // riga in vettore
			// grazie al fatto che leggo solo
			input[i][0] = temp[0];
			input[i][1] = temp[1];
			riga = buffer.readLine();
			i++;
		}

		data = input[0][1];
		dir = input[1][1]; //directory di output
		replicabile = input[2][1];
		excel = Boolean.valueOf(input[3][1]);
		n_vcod_classe_str = input[4][1]; //numero di v_codice classe
		delta = Double.valueOf(input[5][1]);
		lr = Double.valueOf(input[6][1]);
		percentuale_train = Integer.valueOf(input[7][1]);
		costoStr = input[8][1];
		seed = Long.valueOf(input[9][1]);
		epoche = Integer.valueOf(input[10][1]);
		flag_iteraz = Boolean.valueOf(input[11][1]); 
		n_bag = Integer.valueOf(input[12][1]); //numero di iterazioni di BVQ
		dim_bag = Integer.valueOf(input[13][1]);
		flag_SOM = Boolean.valueOf(input[14][1]);
		epocheSOM = Integer.valueOf(input[15][1]);
		
		System.out.println("DATI INSERITI:");
		System.out.println("Nome del file da cui prendere i dati: "+data);
		System.out.println("Directory di output: " +dir);
		System.out.println("Test replicabile: " + replicabile);
		System.out.println("Risultato anche in file excel: " + excel);
		System.out.println("Numero di v_codice per classe " +n_vcod_classe_str);
		System.out.println("Delta: "+delta);
		System.out.println("Lr: "+lr);
		System.out.println("Percentuale di dati presi per il train: "+percentuale_train+"%");
		System.out.println("Matrice di costo: "+costoStr);
		System.out.println("Seed: " + seed);
		System.out.println("Numero di epoche: "+epoche);
		System.out.println("Utilizzo di iterazioni: "+flag_iteraz);
		System.out.println("Numero di bag: " + n_bag);
		System.out.println("Dimensione delle bag: " + dim_bag);
		System.out.println("Utilizzo di SOM: "+flag_SOM);
		System.out.println("Numero di epoche SOM: "+epocheSOM);
		System.out.println();

		// riporto il buffer alla prima posizione, ovvero all'inizio del file
		file.getChannel().position(0);
	}


	// ********************************************************************************************************************************************************************
	// 		Conta il numero di classi che compaiono nel file
	// ********************************************************************************************************************************************************************
	public static int conta_classi(BufferedReader buffer, FileInputStream file) throws IOException{
		String riga;
		String[] dati; // e' la stringa riga separata per ascissa, ordinata e classe

		riga = buffer.readLine();
		while (riga != null){
			dati=riga.split(","); // riga in vettore
			labels.add(Integer.valueOf(dati[1])); // creo una lista contenente tutte le ricorrenze della classe (primo elemento)
			riga=buffer.readLine();
		}
		Collections.sort(labels); // ordino la lista dalla classe piu' piccola alla piu' grande
		int ultimo_elem = labels.get(labels.size() - 1); // prendo l'ultimo elemento della lista che corrisponde al numero di classi del file

		System.out.println("Numero di classi: " + ultimo_elem);

		// riporto il buffer alla prima posizione, ovvero all'inizio del file
		file.getChannel().position(0);
		buffer = new BufferedReader(new InputStreamReader(file,"UTF-8"));
		return ultimo_elem;
	}

	// ********************************************************************************************************************************************************************
	// 		Conta il numero di coordinate
	// ********************************************************************************************************************************************************************
	public static int conta_coordinate(BufferedReader buffer, FileInputStream file) throws IOException{
		String riga;
		String[] dati; // e' la stringa riga separata per ascissa, ordinata e classe

		riga = buffer.readLine();
		dati=riga.split(","); // riga in vettore

		int temp=dati.length-2;
		System.out.println("Numero di coordinate: "+ temp);

		// riporto il buffer alla prima posizione, ovvero all'inizio del file
		file.getChannel().position(0);
		buffer = new BufferedReader(new InputStreamReader(file,"UTF-8"));

		return dati.length-2;
	}

	// ********************************************************************************************************************************************************************
	// 		Conta le righe complessive del file, le righe di classe c1, c2, ecc...
	// ********************************************************************************************************************************************************************
	public static void conta_righe(int classi) throws IOException { 
		occorrenze.add(labels.size()); // nella posizione 0 avro' il numero di righe del file
		System.out.println("Numero di elementi totali: " + occorrenze.get(0));
		for(int i=1; i<=classi; i++){
			occorrenze.add(Collections.frequency(labels, i)); //nella posizione 1 avro' il numero di occorrenze della classe 1, nella posizione 2 avro' il numero di occorrenze della classe 2 ecc...
			System.out.println("Numero di occorrenze della classe "+i+": "+occorrenze.get(i));
		}
	}

	// ******************************************************************************************************************************************************************** 
	//       Valorizzo la matrice con i dati presi dal file 
	// ********************************************************************************************************************************************************************
	public static void istanzia_matrici(BufferedReader buffer, FileInputStream file, double[][][] matrice) throws IOException {
		// riporto il buffer alla prima posizione, ovvero all'inizio del file (Se non lo metto, mi inizia a leggere dalla seconda riga)
		file.getChannel().position(0);
		buffer = new BufferedReader(new InputStreamReader(file));

		String riga;
		String[] dati; // e' la stringa riga separata per ascissa, ordinata e classe
		int i=0; // indice riga della matrice, della classe c1 e c2

		int[] indici = new int[occorrenze.size()];
		for(int j=0;j<occorrenze.size();j++) 
			indici[j] = 0; // #elementi = #elementi classe+1. Inizializzo il vettore con tutti 0. Mi serve come indice

		riga = buffer.readLine(); // inizializzo riga con la prima riga del file
		while (riga != null) { // fino alla fine del file
			// dati e' un vettore di dimensione 3 composto da: ascissa, ordinata, classe
			dati=riga.split(" ");
			for(int j=0; j<coordinate+1;j++){
				// salvo la riga nella matrice 0 (ovvero quella totale)
				matrice[0][i][j] = Double.valueOf(dati[j]);
				// salvo la riga nella matrice esatta (coordinate e' l'elemento dove compare la classe)
				matrice [Integer.valueOf(dati[coordinate])] [indici[Integer.valueOf(dati[coordinate])]] [j] = Double.valueOf(dati[j]);
			}
			indici[Integer.valueOf(dati[coordinate])]++; // indici[leggo l'uultimo elemento della riga]
			i++;
			// Passo alla lettura della riga successiva
			riga=buffer.readLine();
		}
		// riporto il buffer alla prima posizione, ovvero all'inizio del file
		file.getChannel().position(0);
		buffer = new BufferedReader(new InputStreamReader(file));
	}

	// ********************************************************************************************************************************************************************
	// Scrittura su file csv
	// ********************************************************************************************************************************************************************

	public static void scrivi(String scrivi) {
		try {
			fw.write(scrivi);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}		
	
	// ********************************************************************************************************************************************************************
	//            Stampa la matrice data
	// ********************************************************************************************************************************************************************
	public static void stampa_matrice(double[][][] matrice, int classe){
		if(classe == 0) System.out.println("la matrice completa e': ");
		else System.out.println("la matrice di classe "+classe+" e': ");

		for(int j=0;j<occorrenze.get(classe);j++){
			System.out.println(matrice[classe][j][0] +" "+matrice[classe][j][1] +" "+ (Double.valueOf((matrice[classe][j][2]))).intValue() );
		}
		System.out.println();
	}

	public static void stampa_matrice(double[][] matrice, int classe){
		System.out.println("la matrice di classe "+classe+" e': ");

		for(int j=0;j<occorrenze.get(classe);j++){
			System.out.println(matrice[j][0] +" "+matrice[j][1] +" "+ (Double.valueOf((matrice[j][2]))).intValue() );
		}
		System.out.println();
	}

	// ****************************************** OVERLOAD ****************************************
	public static void scrivo_cella(Sheet foglio, int riga, int colonna, String valore, short colore)
	{
		Row row = foglio.getRow(riga);
		// Creiamo una riga
		if(row == null) row = foglio.createRow((short)riga);

		// Creiamo una cella con un valore
		Cell cell = row.createCell(colonna);
		cell.setCellValue(valore);
		// setto lo stile
		CellStyle style = wb.createCellStyle();
		style.setFillForegroundColor(colore);  
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);  
		cell.setCellStyle(style);
	}
	public static void scrivo_cella(Sheet foglio, int riga, int colonna, double valore, short colore)
	{
		Row row = foglio.getRow(riga);
		// Creiamo una riga
		if(row == null) row = foglio.createRow((short)riga);

		// Creiamo una cella con un valore
		Cell cell = row.createCell(colonna);
		cell.setCellValue(valore);
		// setto lo stile
		CellStyle style = wb.createCellStyle();
		style.setFillForegroundColor(colore);  
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);  
		cell.setCellStyle(style);
	}
	public static void scrivo_cella(Sheet foglio, int riga, int colonna, String valore)
	{
		Row row = foglio.getRow(riga);
		if(row == null) row = foglio.createRow((short)riga);
		// Creiamo una cella con un valore
		Cell cell = row.createCell(colonna);
		cell.setCellValue(valore);
	}

	public static void scrivo_cella(Sheet foglio, int riga, int colonna, double valore)
	{
		Row row = foglio.getRow(riga);
		if(row == null) row = foglio.createRow((short)riga);
		// Creiamo una cella con un valore
		Cell cell = row.createCell(colonna);
		cell.setCellValue(valore);
	}
	// ************************************ fine overload **********************************

	public static void scrivo_console_tempo(long end, long start, String stringa)
	{
		long totalTime = end - start;
		long totalTime_milliseconds = totalTime/1000000;

		long hours = totalTime_milliseconds / 3600000;
		long minutes = ((totalTime_milliseconds/1000) % 3600) / 60;
		long seconds = (totalTime_milliseconds/1000) % 60;
		long milliseconds = totalTime_milliseconds % 1000;

		String timeString = String.format(stringa, hours, minutes, seconds, milliseconds);

		System.out.println(timeString);

	}

	public static void scrivo_cella_tempo(Sheet foglio, int riga, int colonna, long end, long start, String stringa)
	{
		long totalTime = end - start;
		long totalTime_milliseconds = totalTime/1000000;

		long hours = totalTime_milliseconds / 3600000;
		long minutes = ((totalTime_milliseconds/1000) % 3600) / 60;
		long seconds = (totalTime_milliseconds/1000) % 60;
		long milliseconds = totalTime_milliseconds % 1000;

		String timeString = String.format("%02d:%02d:%02d:%02d", hours, minutes, seconds, milliseconds);

		scrivo_cella(sheet1,riga,colonna,stringa);
		scrivo_cella(sheet1,riga,colonna+1,timeString);

	}

	// ************************* Supporto ************************
	public static double[][] append(double[][] a, double[][] b) {
		double[][] result = new double[a.length + b.length][];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	public static int[] append(int[] a, int[] b) {
		int[] result = new int[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	public static double[][] cut(double[][] matrice, int righe){
		double[][] matricen= new double[righe][matrice[0].length];
		for(int i=0; i<righe; i++){
			matricen[i]=matrice[i];
		}
		return matricen;
	}
	// *************************************************************

	// ****************************************************** Funzioni Spark **********************************************************

	public static void contaclasse(Double[] datix, LongAccumulator contaclassix){

		if((datix[datix.length -1])>contaclassix.count()){
			System.out.println("datix classe vale " + (datix[datix.length -1]) + "conta classi vale " + contaclassix.count());
			contaclassix.add(1);
			System.out.println("datix classe vale " + (datix[datix.length -1]) + "conta classi vale " + contaclassix.count() + "nuovo");
		}
	}

	public static LongAccumulator sommaAccum(Double[] elem, LongAccumulator accum){
		accum.add(1);
		return accum;
	}

	public static double[] featureToDoubleVec(org.apache.spark.sql.Row feature)
	{
		int dim_feature = feature.size()-2;
		double[] risultato = new double[dim_feature];
		for (int i=0; i<dim_feature; i++) {
			risultato[i] = feature.getDouble(i+2);
		}
		return risultato;
	}
	// **********************************************************************************************************************************

	
	
	// ************************************************** Funzione che esegue la iniz e la inizSOM ************************************************
	private static void eseguiIniz(int classe, Dataset<org.apache.spark.sql.Row> classe_train) {

        filt = classe_train.filter(classe_train.col("label").equalTo(classe)).cache();

        InizSOM oggetto_classe = new InizSOM(percentuale_train, n_vcod_classe[classe-1]);

        oggetto_classe.inizializzaTS(filt, seed);

        //Preparo le variabili per il BVQ
        W=append(W, oggetto_classe.getPesi());
        L=append(L, oggetto_classe.getLabel());

    }
	
	private static void eseguiSOM(int classe, Dataset<org.apache.spark.sql.Row> classe_train) {
		
		filt = classe_train.filter(classe_train.col("label").equalTo(classe)).cache();
		
		InizSOM oggetto_classe = new InizSOM(percentuale_train, n_vcod_classe[classe-1]);

		oggetto_classe.inizializzaTS(filt, seed);
		
		// Scrivo i dati su excel
		for(int i=0;i<oggetto_classe.getPesi().length;i++)
		{
			for(int j=0;j<oggetto_classe.getPesi()[0].length;j++)
			{
				pesi_prima_som[i+indice_csv][j] = oggetto_classe.getPesi()[i][j]+"";
			}
			pesi_prima_som[i+indice_csv][oggetto_classe.getPesi()[0].length] = oggetto_classe.getLabel()[i]+"";
		}
		
		//la prossima volta che verrà richiamata la funzione eseguiSom, sarà per calcolare i pesi della classe successiva e quindi dovrò incrementare il contatore per far si che non mi sovrascriva i dati prima scritti
		indice_csv=indice_csv+n_vcod_classe[classe-1];
		//Start timer som
		long startTime_SOM = System.nanoTime();

		System.out.println("\n************************************* Training SOM: classe "+ oggetto_classe.getLabel()[0] + " *************************************");

		
		TrainSOM som = new TrainSOM(oggetto_classe.getPesi(), oggetto_classe.getLabel(), epocheSOM, lr);
		som.train(filt, flag_iteraz, seed, n_bag, dim_bag, replicabile);
		
		// salvo il tempo di conclusione dell'app
		long endTime_SOM = System.nanoTime();
		totalTime_SOM_list.add(endTime_SOM - startTime_SOM);

		//Preparo le variabili per il BVQ
		W=append(W, som.getweight());
		L=append(L, som.getlabel());
	}
	// **********************************************************************************************************************************
}