public class TestSOM {
	double[][] matrice;
	double[][] punto;
	
	TestSOM (int righe, int colonne){
		matrice = new double[righe][colonne+1];
		punto = new double[1][colonne];
		
		//popolo la matrice
		for(int i=0; i<righe; i++){
			for(int j=0;j<colonne; j++)
				matrice[i][j] = Funx.randDouble(0, 1);
			matrice[i][colonne] = 0;
		}
		//popolo il punto
		for(int i=0;i<punto[0].length;i++)
			this.punto[0][i]=Funx.randDouble(0, 1);
	}
	

	public void run(){
		int[] label = new int[1];
		TrainSOM som = new TrainSOM(punto, label, 1, 0.05);
		som.train(matrice, true);
		
		double[][] pt_trained = new double[1][punto[0].length];
		pt_trained = som.getweight();
		System.out.println("\n**************************************** Risultati TestSOM ***************************************");
		System.out.print("Vecchio punto:\t");
		for(int j=0;j<punto[0].length;j++)
			System.out.print("\t"+punto[0][j]);
		System.out.println();
		System.out.print("Nuovo punto:\t");
		for(int j=0;j<punto[0].length;j++)
			System.out.print("\t"+pt_trained[0][j]);
		System.out.println();
		System.out.println("**************************************************************************************************\n");
	}
}
