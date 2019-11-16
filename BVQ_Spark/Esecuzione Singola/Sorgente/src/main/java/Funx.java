import java.util.Random;

public class Funx {

	// randomizzazione

	public static int randInt(int min, int max) {
		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
	
	public static double randDouble(double min, double max) {
		Random rand = new Random();
		double randomNum = min + (max - min) * rand.nextDouble();
		return randomNum;
	}

	public static double[][] RandomizzaMatrice(double[][] matrice0){
		Random rand = new Random();  // generatore random di numeri  
		double[][] result = new double[matrice0.length][matrice0[0].length];
		for(int i=0; i<matrice0.length; i++)
			for(int j=0; j<matrice0[0].length; j++)
				result[i][j]=matrice0[i][j];
		
		for (int i=0; i<result.length; i++) {
			int posizioneRand = rand.nextInt(result.length); // prende una posizione random tra 0 e la lunghezza della matrice
			double[] temp = result[i];
			result[i] = result[posizioneRand];
			result[posizioneRand] = temp;
		}
		return result;
	}

	//funzioni con vettori e matrici

	public static int[] zeros(int r){
		int[] result = new int[r];
		for (int i = 0; i < r; i++)
			result[i] = 0;
		return result;
	}
	// crea una matrice di zeri r=riga, c=colonna
	public static double[][] zeros(int r, int c) {
		double [][] result = new double [r][c];
		for (int l = 0; l < r; l++) 
			for (int i = 0; i < c; i++)
				result[l][i] = 0;
		return result;
	}
	public static int[][] zeros_int(int r, int c) {
		int [][] result = new int [r][c];
		for (int l = 0; l < r; l++) 
			for (int i = 0; i < c; i++)
				result[l][i] = 0;
		return result;
	}

	// somma degli elem di un vettore
	public static double som(double[] v){
		double result = 0;
		for(int i=0; i<v.length-1; i++) 
			result=result+v[i];
		return result;
	}

	// distanzav fra due punti al quadrato
	public static double distanzavquad(double[] w, double[] p) {
		int x = w.length;
		int y = p.length;
		double result = 0;
		if(x == y)
			for (int i = 0; i < x; i++)
				result = result + Math.pow((w[i]-p[i]), 2);
		else{
			// @todo gestione errore
		}
		return result;
	}

	// distanza fra due punti
	public static double distanzav(double[] w, double[] p) {
		return  Math.pow(distanzavquad(w, p), 0.5);
	}

	// Somma degli elementi dei vettori membro a membro
	public static double[] addv(double[] arr1, double[] arr2) {
		int x = arr1.length;
		int y = arr2.length;
		double[] result = new double[x];
		if(x == y){
			for(int i=0; i<x; i++) 
				result[i]=arr1[i]+arr2[i];
		}
		else{
			// @todo gestire eccezione
		}
		return result;
	}

	// differenza fra vettori membro a membro
	public static double[] diffv(double[] arr1, double[] arr2){
		double[] result;
		int x = arr1.length;
		int y = arr2.length;
		result = new double[x];
		if(x == y)
			for(int i=0; i<x; i++) 
				result[i]=arr1[i]-arr2[i];
		else{
			// @todo gestione errore
		}
		return result;
	}

	// sommatoria del prodotto fra vettori membro a membro
	public static double dotprod(double[] arr1, double[] arr2){
		double result =0;
		int x = arr1.length;
		int y = arr2.length;
		double[] arr3= new double[x];
		if(x == y){
			for(int i=0; i<x; i++) 
				arr3[i]=arr1[i]*arr2[i];
			result = som(arr3);
		}
		else{
			// @todo gestire eccezione
		}
		return result;
	}

	// prodotto di uno scalare per ogni elemento del vettore
	public static double[] prodxscal(double x, double[] v){
		int j = v.length;
		double[] result = new double[j];
		for(int i=0; i<j; i++) {
			result[i]=x*v[i];
		}
		return result;
	}

	// QUICK SORT
	public static double[][] indexsort(double[][] inputArr) {

		if (inputArr == null || inputArr.length == 0) {
			//return; exception handler
		}
		int length = inputArr.length;
		inputArr = quickSort( inputArr, 0, length - 1);
		return inputArr;
	}

	private static double[][] quickSort(double[][]inputArr, int lowerIndex, int higherIndex) {

		int i = lowerIndex;
		int j = higherIndex;
		// calculate pivot number, I am taking pivot as middle index number
		double pivot = inputArr[lowerIndex+(higherIndex-lowerIndex)/2][0];
		// Divide into two arrays
		while (i <= j) {
			/**
			 * In each iteration, we will identify a number from left side which 
			 * is greater then the pivot value, and also we will identify a number 
			 * from right side which is less then the pivot value. Once the search 
			 * is done, then we exchange both numbers.
			 */
			while (inputArr[i][0] < pivot)
				i++;
			while (inputArr[j][0] > pivot)
				j--;
			if (i <= j) {
				inputArr = exchangeNumbers(inputArr, i, j);
				//move index to next position on both sides
				i++;
				j--;
			}
		}
		// call quickSort() method recursively
		if (lowerIndex < j)
			inputArr = quickSort(inputArr, lowerIndex, j);
		if (i < higherIndex)
			inputArr = quickSort(inputArr, i, higherIndex);
		return inputArr;
	}

	private static double[][] exchangeNumbers(double[][] inputArr, int i, int j) {
		double[] temp = inputArr[i];
		inputArr[i] = inputArr[j];
		inputArr[j] = temp;
		return inputArr;
	}

	// Selection sort
	public static double[][] selectionSort(double[][] arr) 
	{
		for (int i=0; i<arr.length-1; i++){
			int index = i;
			for (int j=i+1; j<arr.length; j++)
				if (arr[j][arr[0].length-1] < arr[index][arr[0].length-1]) // se nella riga j la classe e' minore della riga i, allora...
					index = j;
			double[] temp = new double[3];
			for (int z=0; z<arr[0].length; z++){
				temp[z] = arr[index][z];
				arr[index][z] = arr[i][z];
				arr[i][z] = temp[z];
			}
		}
		return arr;
	}
}
