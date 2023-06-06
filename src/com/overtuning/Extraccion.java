package com.overtuning;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Logger;
import org.json.JSONException;
import com.overtuning.Overt.estaciones;
import com.overtuning.Overt.lectura;

public class Extraccion {
	
public static void busqueda_lecturas(List < estaciones > estaciones) throws JSONException, IOException{ 
		
		Logger logger = Logger.getLogger("etl_log");
		String dateInit   = Utils.get_date_start_today();
		String dateFinish =  Utils.get_date_now();
		int encountered = 0;
		List < lectura > lecturas = new ArrayList < lectura > ();		
		Calendar fecha = Calendar.getInstance();		
        String date_file = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", fecha);		
        PrintWriter pw= new PrintWriter(new File("C:\\datos\\muestras24h\\extraccion_"+date_file+"_bombeos_ld.csv"));
        StringBuilder sb=new StringBuilder();        
		/**************************************************************/
        System.out.println(" for " + (estaciones.size()) + " tags..");		
		/**************************************************************/		
        String URL = Credenciales.SQLSERVER.URL;
	    String database =Credenciales.SQLSERVER.database;
	    String user = Credenciales.SQLSERVER.user;
	    String pwd = Credenciales.SQLSERVER.pwd;	  
	    String SQL;
	    /**********************************************************/	    
	    String connectionUrl = URL + database + user + pwd;	  
	    logger.info(" > ="+ dateInit +" & < ="+ dateFinish );   
		
	     sb.append("Estacion");
    	 sb.append(",");
    	 sb.append("DateTime");
    	 sb.append(",");
    	 sb.append("Caudal");
    	 sb.append(",");
    	 sb.append("Conductividad");
    	 sb.append(",");
    	 sb.append("Nivel");
    	 sb.append(",");
    	 sb.append("pH");
    	 sb.append(",");
    	 sb.append("Temperatura");  
    	 sb.append(",");
    	 sb.append("Caudal");  
    	 sb.append("\r\n");	     	 

	    	
	   try {
            Class.forName(Credenciales.SQLSERVER.clase);
            Connection con = DriverManager.getConnection(connectionUrl);
            Statement stmt = con.createStatement();
            for (int i = 0; i <= estaciones.size() - 1; i++)
            {
                encountered = 0;             
                SQL = "select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura,VA from dbo.PozosSMA_GPConsultores  where TIMESTAMP > '"+estaciones.get(i).time+"' AND  TIMESTAMP <=  '"+Utils.get_date_now()+"' AND IDEstacion = '"+estaciones.get(i).nombre+"';";
                ResultSet rs = stmt.executeQuery(SQL);
                while (rs.next())
                {
                	
                	 sb.append(rs.getString(1));
                	 sb.append(",");
                	 sb.append(rs.getString(2));
                	 sb.append(",");
                	 sb.append(rs.getString(3));
                	 sb.append(",");
                	 sb.append(rs.getString(4));
                	 sb.append(",");
                	 sb.append(rs.getString(5));
                	 sb.append(",");
                	 sb.append(rs.getString(6));
                	 sb.append(",");
                	 sb.append(rs.getString(7));
                	 sb.append(",");
                	 sb.append(rs.getString(8));
                	 sb.append("\r\n");                    
                	 lecturas.add(new lectura(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7),rs.getString(8)));                   
                     encountered++;
                }
               
                System.out.println(" for " +estaciones.get(i).nombre + " >" +estaciones.get(i).time  + " & <"+ Utils.get_date_now()+" Process => " + Utils.percentage(i, estaciones.size() - 1) + " Result => " + encountered);
            }
            pw.write(sb.toString());
            pw.close();
            con.close();
            System.out.println("Connection with database SQL Server is closed");
        } catch (Exception e) {
        	
           logger.severe("error connection to MLCC database server " + e);
           System.out.println("error connection to MLCC database server " + e);           
           SendMail.send_msg("error-etl-respaldo", "error connection to MLCC database server : " + e); 

        }
        logger.info("total entries encountered in mlcc database : " + lecturas.size());
        System.out.println("total entries encountered : " + lecturas.size());        
        if(lecturas.size()>0)
        {
        	
        	 Transaccion.load_remote_server(lecturas);
        	 logger.info("Finish ETL "); 
        	 System.out.println("Fin");
        }
        else
        {        	
             System.out.println("No se encontraron lecturas en el periodo consultado... ");
             logger.info("No se encontraron lecturas en el periodo consultado... ");           
           
        }
        logger.info("finish");
        System.out.println("finish");
      		
		
	}

}
