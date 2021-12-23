package com.overtuning;

import java.util.Calendar;

public class Utils {
	
	public static String get_date_finish(){
		
		
		  Calendar fecha = Calendar.getInstance();	
		  String actual_date = String.format("%1$tY-%1$tm-%1$td 00:00:00", fecha);
		  return actual_date;
	
		
	}
	 public static String  get_date_start(){
			
			
	  	  Calendar fecha = Calendar.getInstance();  	  
	  	  fecha.add(Calendar.HOUR, -24);
	  	  String actual_date = String.format("%1$tY-%1$tm-%1$td 00:00:00", fecha);
	      return actual_date;    	
	  	
			
		}
	 
	 
	 public static String  get_date_start_today(){
			
			
	  	  Calendar fecha = Calendar.getInstance();	  
	  	  String actual_date = String.format("%1$tY-%1$tm-%1$td 00:00:00", fecha);
	      return actual_date;    	
	  	
			
		}
	 
	 
	 
	 public static String  get_date_now(){
		 
		  Calendar fecha = Calendar.getInstance(); 	 
	  	  String actual_date = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", fecha);		
	      return actual_date;   
		 
	 }
	    
	   
	    
	    public static String percentage(int contador, int lecturas) {
	        double p = contador * 100 / lecturas;
	        return p + " %";
	    }
	    public static String convert_ce(String conductividad) {
	        if (conductividad != null) {

	            float f = Float.parseFloat(conductividad) * 1000;
	            return String.format("%.0f", f);
	        } else {

	            return null;
	        }

	    }
	    public static String convert_2f(String dato) {

	        if (dato != null) {

	            float f = Float.parseFloat(dato) * 1;
	            return String.format("%.2f", f);

	        } else {

	            return null;
	        }

	    }
		public static String get_date_lector() {
			// TODO Auto-generated method stub
			  Calendar fecha = Calendar.getInstance();     		  
	  		  String actual_date = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", fecha);    	
	  		  return actual_date;    
		}

}
