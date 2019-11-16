import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;

public class TrainSOM implements Serializable {

	static private double[][] weight;
	static private int[] label;
	private int epoch; //numero di epoche;
	private double lr0; //velocitï¿½ di apprendimento
	public int k_utile;

	//*****************************************************************************************************************************
	// Funzioni pubbliche
	//*****************************************************************************************************************************
	public TrainSOM(double[][] weight, int[] label, int epoch, double lr0){
		// Utilizzo il doppio for in quanto senno' weight viene passato per riferimento, invece in questo modo viene fatta un'assegnazione
		TrainSOM.weight = new double[weight.length][weight[0].length];
		for(int i=0; i<weight.length; i++)
			for(int j=0; j<weight[0].length; j++)
				TrainSOM.weight[i][j]=weight[i][j];

		TrainSOM.label= new int[label.length];
		for(int i=0;i<label.length; i++)
			TrainSOM.label[i] = label[i];

		this.epoch=epoch;
		this.lr0=lr0;
	}

	public void setweight(double[][] weight){
		TrainSOM.weight = new double[weight.length][weight[0].length];
		for(int i=0; i<weight.length; i++)
			for(int j=0; j<weight[0].length; j++)
				TrainSOM.weight[i][j]=weight[i][j];
	}

	public double[][] getweight(){
		return weight;
	}

	public void setlabel(int[] label){
		TrainSOM.label= new int[label.length];
		for(int i=0;i<label.length; i++)
			TrainSOM.label[i] = label[i];
	}

	public int[] getlabel(){
		return label;
	}

	public void setepoche(int epoche){
		epoch = epoche;
	}

	public int getepoche(){
		return epoch;
	}

	public void setLr0(double lr0){
		this.lr0 = lr0;
	}

	public double getLr0(){
		return this.lr0;
	}

	// allena la rete don il dataset dato
	public void train(double[][] pset, boolean flag_iteraz) {
		k_utile=0;
		for(int i=0; i<epoch; i++)
			for(int j=0; j<pset.length; j++)
				learn(pset[j]);

	}

	// allena la rete don il dataset dato
    public void train(Dataset<org.apache.spark.sql.Row> pset, boolean flag_iteraz, long seed, int num_bag, int dim_bag, String replicabile) {
        k_utile=0;
        pset.cache();
		long dim_pset = pset.count();
		
        switch(replicabile) {
        
        case "false":
        	if(flag_iteraz){
        		for(int i=0; i<num_bag; i++){
        			Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 1D*dim_bag/dim_pset, seed+i).limit(dim_bag); //prendo dim_bag elementi random alla volta
					p.cache();
					p.foreach((ForeachFunction<org.apache.spark.sql.Row>) q -> learn(q));
        		}
        	}else{
	            for(int i=0; i<epoch; i++) {
	            	pset.foreach((ForeachFunction<org.apache.spark.sql.Row>) p -> learn(p));
	            }
        	}
            break;
        case "true":
        	if(flag_iteraz){
        		for(int i=0; i<=num_bag; i++){
        			Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 1D*dim_bag/dim_pset, seed+i).limit(dim_bag); //prendo dim_bag elementi random alla volta
					p.collect();
					p.cache();
					p.foreach((ForeachFunction<org.apache.spark.sql.Row>) q -> learn(q));
        		}
        	}else{
	            for(int i=0; i<epoch; i++){
	                pset.collect();
	                pset.foreach((ForeachFunction<org.apache.spark.sql.Row>) p -> learn(p));
	            }
        	}
            break;
        case "secure":
        	if(flag_iteraz){
				for(int i=0; i<=num_bag*dim_bag; i++){
					Dataset<org.apache.spark.sql.Row> p = pset.sample(false, 0.05, seed+i).limit(1); //prendo un elemento random alla volta (takeSample...1)
					p.cache();
					p.foreach((ForeachFunction<org.apache.spark.sql.Row>) q -> learn(q));
				}
			}else{
	            for(int i=0; i<epoch; i++){
	                for(int j=0; j<dim_pset; j++){
	                	Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 0.05, seed+j).limit(1);
	                	p.cache();
	                    learn(p.first());
	                }
	            }
			}
            break;
        case "secure_index": // Andiamo a prendere deterministicamente i punti per fare train usando l'id per andare a selezionarli
        	if(flag_iteraz){
        		// Abbiamo deciso di prendere gruppi di punti random per velocizzare le iterazioni che sarebbero sennò troppo lente
				for(int i=0; i<num_bag; i++){
					Dataset<org.apache.spark.sql.Row> p = pset.sample(true, 1D*dim_bag/dim_pset, seed+i).limit(dim_bag); //prendo dim_bag elementi random alla volta
					p.cache();
					List<org.apache.spark.sql.Row> IDs;
		        	IDs = p.select("id").collectAsList();
					for(int j=0; j < IDs.size(); j++) { // Ciclo fino a quando il set non avrà dimensione = alla dimensione dim_bag. In pratica alleno con dim_bag*num_bag elementi.
						learn(p.where("id='"+IDs.get(j).getInt(0)+"'").first());
	                }
				}
        	}else{
	        	List<org.apache.spark.sql.Row> IDs;
	        	IDs = pset.select("id").collectAsList();
	        	pset.cache();
	        	for(int i=0; i<epoch; i++){ // Per ogni epoca
	        		for(int j=0; j<IDs.size(); j++) { 
	            		learn(pset.where("id='"+IDs.get(j).getInt(0)+"'").first()); // Alleno con il punto preso dirrettamente con il suo ID
	                }
	            }
        	}
            break;
        default:
            // code block
        }
    }

	//*****************************************************************************************************************************
	// Funzioni private
	//*****************************************************************************************************************************

	private  void learn(double[] pt) {
		/// separo i dati dalla classe
		double[] p = new double[pt.length-1];
		for (int i = 0; i < pt.length-1; i++) p[i] = pt[i];
		// dw come la matrice dei pesi ma di zeri
		// distanze lunga come la matrice dei pesi
		int r = weight.length;
		double[][] d = new double[r][2];

		for (int i = 0; i < r; i++){
			d[i][0] = Funx.distanzav(weight[i], p);
			d[i][1] = i;
		}
		d = Funx.indexsort(d);
		int index1 = (int) d[0][1];
		k_utile++;
		//lr = lr0*(k_utili+1)^(-0.51);
		double lr = lr0*Math.pow((k_utile),-0.51);
		//net.IW{i,j} = net.IW{i,j} - lr*dw;
		weight[index1] = Funx.addv(weight[index1], Funx.prodxscal(lr, Funx.diffv(p, weight[index1])));
	}

	synchronized private void learn(org.apache.spark.sql.Row pt) {
		try{
			int feature_length = pt.size()-2;
			double[] p = new double[feature_length];

			for(int i= 0; i<feature_length; i++)
				p[i] = pt.getDouble(i+2);
			
			int r = weight.length;
			
			double[][] d = new double[r][feature_length];

			for (int i = 0; i < r; i++){
				d[i][0] = Funx.distanzav(weight[i], p);
				d[i][1] = i;
			}
			d = Funx.indexsort(d);
			int index1 = (int) d[0][1];
			k_utile++;
			//lr = lr0*(k_utili+1)^(-0.51);
			double lr = lr0*Math.pow((k_utile),-0.51);
			//net.IW{i,j} = net.IW{i,j} - lr*dw;
			weight[index1] = Funx.addv(weight[index1], Funx.prodxscal(lr, Funx.diffv(p, weight[index1])));
		} catch(Exception e) {
			System.out.println(e.toString());
		}
	}
}
