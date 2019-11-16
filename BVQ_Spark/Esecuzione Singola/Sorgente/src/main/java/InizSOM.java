import java.io.Serializable;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class InizSOM implements Serializable{
	static private double[][] mpesi, mtrain, mtest;
	static private int[] mlabel;
	private int neuroni;
	private int percentuale;
	private long elem_presi = 0; // ogni elemento indica il numero di quanti elementi di quella classe sono stati presi
	private long tr=0;
	private long ts=0;

	public InizSOM(int perc, int neuroni){
		this.percentuale=perc;
		this.neuroni=neuroni;
	}

	public double[][] getPesi(){
		return mpesi;
	}

	public double[][] getTrain(){
		return mtrain;
	}

	public double[][] getTest(){
		return mtest;
	}

	public int[] getLabel(){
		return mlabel;
	}

	// crea la matrice dei pesi, matrice per il train e matrice per il test
	public void inizializzaTS(double[][] matrice) {

		mpesi = new double[neuroni][matrice[0].length-1]; // pesi e' una matrice che contiene i primi neuroni elementi della matrice di classe 1
		mlabel = new int[neuroni];
		mtrain = new double[matrice.length*percentuale/100][matrice[0].length];
		mtest = new double[matrice.length - mtrain.length][matrice[0].length];

		int p=0; // posizione elemento nella matrice p

		for(int i=0;i<matrice.length;i++) { //per tutte le righe
			if(elem_presi < neuroni){
				for(int j=0;j<matrice[0].length-1;j++) //per tutte le colonne tranne l'ultima
					mpesi[p][j]=matrice[i][j];
				mlabel[p]=(int) matrice[i][matrice[0].length-1]; //l'ultima colonna la metto in label
				elem_presi++;
				p++;
				if(tr < mtrain.length){ //metto in train
					for(int j=0;j<matrice[0].length;j++) //per tutte le colonne
						mtrain[(int)tr][j]=matrice[i][j];
					tr++;
				}
				else{ //metto in test
					for(int j=0;j<matrice[0].length;j++) //per tutte le colonne
						mtest[(int)ts][j]=matrice[i][j];
					ts++;
				}
			}
			else if(tr < mtrain.length){ //metto in train
				for(int j=0;j<matrice[0].length;j++) //per tutte le colonne
					mtrain[(int)tr][j]=matrice[i][j];
				tr++;
			}
			else{ //metto in test
				for(int j=0;j<matrice[0].length;j++) //per tutte le colonne
					mtest[(int)ts][j]=matrice[i][j];
				ts++;
			}
		}

	}

	// crea la matrice dei pesi, matrice per il train e matrice per il test
	public void inizializzaTS(Dataset<org.apache.spark.sql.Row> matrice, long seed) {
		mpesi = new double[neuroni][MainClass.coordinate];

		mlabel = new int[neuroni];
		
		Dataset<org.apache.spark.sql.Row> lista = matrice.sample(false, 2D*neuroni/matrice.count(), seed).limit(neuroni);
		lista.foreach((ForeachFunction<org.apache.spark.sql.Row>) elem -> inserisci(elem));
	}

	private void inserisci(Row elem) {
		double label = elem.getDouble(1);

		for(int j=2;j<elem.size();j++) //per tutte le colonne tranne l'ultima
			mpesi[(int)elem_presi][j-2]=(double)elem.getDouble(j);
		
		mlabel[(int)elem_presi]= (int) label; //l'ultima colonna la metto in label
		elem_presi++;
	}
}