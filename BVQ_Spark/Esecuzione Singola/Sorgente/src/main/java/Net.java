import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


@SuppressWarnings("serial")
public class Net implements Serializable{

	static private double[][] weight;
	private double[][] costi;
	private int[] label;
	private int epoch; //numero di epoche;
	private int iteraz; // numero iterazioni per epoche
	private double lr0; //velocità di apprendimento
	private double delta; // dimensione della finestra del bvq
	public int k_utile;
	public int k_err;
	public int k_esatta;
	
	//*****************************************************************************************************************************
	// Funzioni pubbliche
	//*****************************************************************************************************************************
	public Net(double[][] weight, int[] label, double[][] costi, int epoch, int iteraz, double lr0, double delta){
		Net.weight= new double[weight.length][weight[0].length];
		for(int i=0; i<weight.length; i++)
			for(int j=0;j<weight[0].length; j++)
				Net.weight[i][j] = weight[i][j];
		this.label= new int[label.length];
		for(int i=0;i<label.length; i++) 
			this.label[i] = label[i];
		this.costi=new double[costi.length][costi[0].length];
		for(int i=0; i<costi.length; i++)
			for(int j=0;j<costi[0].length; j++)
				this.costi[i][j] = costi[i][j];
		this.epoch=epoch;
		this.iteraz=iteraz;
		this.lr0=lr0;
		this.delta=delta;
	}

	public void setweight(double[][] weight){
		Net.weight= new double[weight.length][weight[0].length];
		for(int i=0; i<weight.length; i++)
			for(int j=0;j<weight[0].length; j++)
				Net.weight[i][j] = weight[i][j];
	}

	public double[][] getweight(){
		return Net.weight;
	}

	public void setcost(double[][] costi){
		this.costi=new double[costi.length][costi[0].length];
		for(int i=0; i<costi.length; i++)
			for(int j=0;j<costi[0].length; j++)
				this.costi[i][j] = costi[i][j];
	}

	public double[][] getcost(){
		return this.costi;
	}

	public void setlabel(int[] lab){
		this.label= new int[label.length];
		for(int i=0;i<label.length; i++)
			this.label[i] = label[i];
	}

	public int[] getlabel(){
		return this.label;
	}

	public void setepoche(int epoche){
		this.epoch = epoche;
	}

	public int getepoche(){
		return this.epoch;
	}

	public void setiteraz(int iterazioni){
		this.iteraz = iterazioni;
	}

	public int geteiteraz(){
		return this.iteraz;
	}

	public void setLr0(double lr0){
		this.lr0 = lr0;
	}

	public double getLr0(){
		return this.lr0;
	}

	public void setDelta(double delta){
		this.delta = delta;
	}

	public double getDelta(){
		return this.delta;
	}

	public void train(Dataset<org.apache.spark.sql.Row> pset, boolean flag_iteraz, long seed, int num_bag, int dim_bag, String replicabile) throws IOException, UnsupportedEncodingException {
		k_utile=0;
		pset.cache();
		long dim_pset = pset.count();
		
		try{

			switch(replicabile) {

			case "false":
				if(flag_iteraz){
					// Abbiamo deciso di prendere gruppi di punti random per velocizzare le iterazioni che sarebbero sennò troppo lente
					for(int i=0; i<num_bag; i++){
						Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 1D*dim_bag/dim_pset, seed+i).limit(dim_bag); //prendo dim_bag elementi random alla volta
						p.cache();
						p.foreach((ForeachFunction<org.apache.spark.sql.Row>) s -> learn(PointToDoubleVec2(s)));
					}
				}else{
					for(int i=0; i<epoch; i++)
						pset.foreach((ForeachFunction<org.apache.spark.sql.Row>) p -> learn(PointToDoubleVec2(p)));
				}
				break;
			case "true":
				if(flag_iteraz){
					for(int i=0; i<num_bag; i++){
						Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 1D*dim_bag/dim_pset, seed+i).limit(dim_bag); //prendo dim_bag elementi random alla volta
						p.collect();
						p.cache();
						p.foreach((ForeachFunction<org.apache.spark.sql.Row>) q -> learn(PointToDoubleVec2(q)));
					}
				}else{
					for(int i=0; i<epoch; i++)
						pset.collect();
						pset.foreach((ForeachFunction<org.apache.spark.sql.Row>) p -> learn(PointToDoubleVec2(p)));
				}
				break;

			case "secure":
				if(flag_iteraz){
					for(int i=0; i<num_bag*dim_bag; i++){
						Dataset<org.apache.spark.sql.Row> p = pset.sample(false, 0.05, seed+i).limit(1); //prendo un elemento random alla volta (takeSample...1)
						p.cache();
						p.foreach((ForeachFunction<org.apache.spark.sql.Row>) q -> learn(PointToDoubleVec2(q)));
					}
				}else{
					for(int i=0; i<epoch; i++){
						for(int j=0; j<dim_pset; j++){
							Dataset<org.apache.spark.sql.Row> p = pset.sample(false, 0.05, seed+j).limit(1);
							p.cache();
							p.foreach((ForeachFunction<org.apache.spark.sql.Row>) q -> learn(PointToDoubleVec2(q)));
						}
					}
				}
				break;
			case "secure_index":
				if(flag_iteraz){
					// Abbiamo deciso di prendere gruppi di punti random per velocizzare le iterazioni che sarebbero sennò troppo lente
					for(int i=0; i<num_bag; i++){
						Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 1D*dim_bag/dim_pset, seed+i).limit(dim_bag); //prendo dim_bag elementi random alla volta
						p.cache();
						List<org.apache.spark.sql.Row> IDs;
			        	IDs = p.select("id").collectAsList();
						for(int j=0; j < IDs.size(); j++) { // Ciclo fino a quando il set non avrà dimensione = alla dimensione dim_bag. In pratica alleno con dim_bag*num_bag elementi.
							learn(PointToDoubleVec2(p.where("id='"+IDs.get(j).getInt(0)+"'").first()));
		                }
					}
				}else{
					List<org.apache.spark.sql.Row> IDs;
		        	IDs = pset.select("id").collectAsList();
		        	pset.cache();
					for(int i=0; i<epoch; i++)
						for(int j=0; j < IDs.size(); j++) { // Ciclo fino a quando il set non avrà dimensione = alla dimensione del training set. In pratica alleno con tutti gli elementi.
							learn(PointToDoubleVec2(pset.where("id='"+IDs.get(j).getInt(0)+"'").first()));
		                }
				}
				break;
			default:
				// code block
			}
		}
		catch(Exception e) {
		}
	}

	// ritorna la lista delle classi decise per ogni punto
	public  int[] test(double[][] pset) {
		int[] results = new int[pset.length];
		k_err=0;
		k_esatta=0;
		for(int j=0; j<pset.length; j++){
			results[j] = valuta(pset[j]);
		}
		return results;
	}

	//*****************************************************************************************************************************
	// Funzioni private
	//*****************************************************************************************************************************

	// 1-nearest neighbor tra il punto e la matrice dei pesi
	private  int valuta(double[] pt) {
		double[] p = new double[pt.length-1];
		int classp = (int) pt[pt.length-1];
		for (int i = 0; i < pt.length-1; i++)
			p[i] = pt[i];
		int r = weight.length;
		double[][] d = new double[r][2];
		for (int i = 0; i < r; i++){
			d[i][0] = Funx.distanzavquad(weight[i], p);
			d[i][1] = i;
		}
		d = Funx.indexsort(d);
		int index1 = (int) d[0][1];

		if(label[index1] != classp) k_err++;
		else k_esatta++;

		return label[index1];
	}

	public double valuta2(double[] pt, double pt_class) {
		int pt_dim = pt.length;
		double[] p = new double[pt_dim];
		for (int i = 0; i < pt_dim; i++)
			p[i] = pt[i];
		int r = weight.length;
		double[][] d = new double[r][2];
		for (int i = 0; i < r; i++){
			d[i][0] = Funx.distanzavquad(weight[i], p);
			d[i][1] = i;
		}
		d = Funx.indexsort(d);
		int index1 = (int) d[0][1];
		
		if(label[index1] != (int)pt_class) k_err++;
		else k_esatta++;

		return label[index1];
	}

	synchronized private void learn(double[] pt) {
		/// separo i dati dalla classe
		int dim_pt = pt.length-1;
		double[] p = new double[dim_pt];
		int classp = (int) pt[dim_pt];
		for (int i = 0; i < dim_pt; i++) 
			p[i] = pt[i];
		// distanze lunga come la matrice dei pesi
		int r = weight.length;
		int c = weight[0].length;

		double[][] d = new double[r][2];

		for (int i = 0; i < r; i++){
			d[i][0] = Funx.distanzav(weight[i], p);
			d[i][1] = i;
		}
		d = Funx.indexsort(d);
		int index1 = (int) d[0][1];
		int index2 = (int) d[1][1];
		if (label[index1] != label[index2]){
			double[] projp= new double[c];
			projp = proiezione(index1, index2, p);
			/// if(dist(p_proj,p)<= delta/2)
			if(Funx.distanzav(p, projp) <= delta/2) {
				k_utile++;
				backPropagation(projp, classp, index1, index2, lr0);
			}
		} 
	}

	// learn con stampa su console
	private  void writed_learn(double[] pt) {
		System.out.println("\n**********************************************************************");
		/// separo i dati dalla classe
		double[] p = new double[pt.length-1];
		int classp = (int) pt[pt.length-1];
		System.out.println("coordinate del punto");
		for (int i = 0; i < pt.length-1; i++){
			p[i] = pt[i];
			System.out.print(p[i] + " ");
		}
		System.out.print(" con classe: "+classp +"\n");

		// distanze lunga come la matrice dei pesi
		int r = weight.length;
		int c = weight[0].length;
		double[][] d = new double[r][2];
		// popolo d don le distanze e gli indici
		for (int i = 0; i < r; i++){
			d[i][0] = Funx.distanzav(weight[i], p);
			d[i][1] = i;
		}
		d = Funx.indexsort(d);

		System.out.println("\ncoordinate --- distanza da p --- indice del vettore");
		for (int i = 0; i < r; i++){
			for (int j = 0; j < c; j++)
				System.out.print(weight[(int)d[i][1]][j] + " ");
			System.out.print(d[i][0] +" "+(int)d[i][1] + "\n");
		}
		System.out.println();

		int index1 = (int) d[0][1];
		int index2 = (int) d[1][1];

		if (label[index1] != label[index2]){
			double[] projp= new double[c];
			projp = proiezione(index1, index2, p);
			System.out.println("distanza da p a projp: "+Funx.distanzav(p, projp));
			System.out.print("proiezione: ");
			for (int i = 0; i < projp.length; i++)
				System.out.print(projp[i]+" ");
			System.out.println();
			if(Funx.distanzav(p, projp) <= delta/2) {
				System.out.println("iterazione utile ");
				k_utile++;
				backPropagation(projp, classp, index1, index2, lr0);
			}
		} 
		if(label[index1] != classp) k_err++;
		else k_esatta++;
	}

	private void backPropagation(double[] projp, int classp, int index1, int index2, double lr0){
		int r = weight.length;
		int c = weight[0].length;

		// dw ha dimensioni come la matrice dei pesi ma viene settata a zero
		double[][] dw = Funx.zeros(r, c);
		/*
    	% vado ad aggiornare soltanto i valori vicini, secondo questa regola. .* e' la moltiplicazione componente per componente. 
        dw(I(1),:)= ((costo(labelp,label(I(2)))-costo(labelp,label(I(1)))).*(w(I(1),:)-p_proj))/dist(w(I(1),:),w(I(2),:)'); % ...meno il punto proiettato, diviso la distanza tra i due valori
        dw(I(2),:)= ((costo(labelp,label(I(1)))-costo(labelp,label(I(2)))).*(w(I(2),:)-p_proj))/dist(w(I(1),:),w(I(2),:)');
		 */
		double coeff1,coeff2;
		coeff1 = (costi[classp-1][label[index2]-1]-costi[classp-1][label[index1]-1])/(Funx.distanzav(weight[index1], weight[index2]));
		dw[index1] = Funx.prodxscal(coeff1,Funx.diffv(weight[index1], projp));
		coeff2 = (costi[classp-1][label[index1]-1]-costi[classp-1][label[index2]-1])/(Funx.distanzav(weight[index1], weight[index2]));
		dw[index2] = Funx.prodxscal(coeff2,Funx.diffv(weight[index2], projp));
		//lr = lr0*(k_utili+1)^(-0.51);
		double lr = lr0*Math.pow((k_utile),-0.51);
		//net.IW{i,j} = net.IW{i,j} - lr*dw;
		weight[index1] = Funx.diffv(weight[index1], Funx.prodxscal(lr, dw[index1]));
		weight[index2] = Funx.diffv(weight[index2], Funx.prodxscal(lr, dw[index2]));
	}


	// proiezione di un punto p
	private double[] proiezione(int index1, int index2, double[] p) {
		//deltamq = sum((w(I(1),:)-w(I(2),:)).^2);
		double deltamq;
		deltamq = Funx.distanzavquad(weight[index1], weight[index2]);
		//a=((sum(w(I(1),:).^2)-sum(w(I(2),:).^2))/2);
		double a;
		a = (Math.pow(Funx.som(weight[index1]), 2) - Math.pow(Funx.som(weight[index2]), 2))/2;
		//b=dotprod((w(I(1),:)-w(I(2),:)),p);
		double b;
		b = Funx.dotprod(Funx.diffv(weight[index1], weight[index2]), p);
		//coeff = (a-b)/deltamq;
		double coeff = (a-b)/deltamq;
		//p_proj = coeff.*(w(I(1),:)-w(I(2),:)) + p';
		int c = weight[0].length;
		double[] projp= new double[c];
		projp = Funx.addv(Funx.prodxscal(coeff,Funx.diffv(weight[index1], weight[index2])), p);

		return projp;
	}

	public static double[] PointToDoubleVec2(Row point)
	{
		// la stringa feature si compone di: numero feature, chiavi, valori
		int dim_point = point.size()-2;
		double[] risultato = new double[dim_point+1]; // feature + label
		
		for(int j=0;j<dim_point;j++)
			risultato[j]=point.getDouble(j+2);
		risultato[dim_point] = point.getDouble(1);
		
		return risultato;
	}

}