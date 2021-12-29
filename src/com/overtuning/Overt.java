package com.overtuning;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger; 


 


class MyFormatter extends Formatter {

    private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");

    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        builder.append(df.format(new Date(record.getMillis()))).append(" - ");
        builder.append("[").append(record.getSourceClassName()).append(".");
        builder.append(record.getSourceMethodName()).append("] - ");
        builder.append("[").append(record.getLevel()).append("] - ");
        builder.append(formatMessage(record));
        builder.append(System.lineSeparator());
        return builder.toString();
    }

    public String getHead(Handler h) {
        return super.getHead(h);
    }

    public String getTail(Handler h) {
        return super.getTail(h);
    }
}


public class Overt {
	
	
	  public static class lectura {

	        private String estacion;
	        private String time;
	        private String caudal;
	        private String conductividad;
	        private String nivel;
	        private String ph;
	        private String temperatura;
	        private String volumen;

	        private lectura(String _Estacion, String _Time, String _Caudal, String _Conductividad, String _Nivel, String _PH, String _Temperatura, String _Volumen ) {

	            estacion = _Estacion;
	            time = _Time;
	            caudal = _Caudal;
	            conductividad = _Conductividad;
	            nivel = _Nivel;
	            ph = _PH;
	            temperatura = _Temperatura;
	            volumen = _Volumen;

	        }

	    }
	  
	  public static class estaciones {

	        private String nombre;	       

	        private estaciones(String _Nombre) {

	            nombre = _Nombre;        

	        }

	    }

	

	public static void main(String[] args) throws SecurityException, IOException {
		// TODO Auto-generated method stub	
		 
		 System.out.println("Script for update entries...");		 
		 configure_logs();
		 get_estaciones_monitoreo();
	}
	
	
	
	 private static void configure_logs() throws SecurityException, IOException {

	        boolean append = false;
	        Logger logger = Logger.getLogger("etl_log");
	        logger.setUseParentHandlers(append);
	        String pathLog = "C:/datos/daemon_logs_update.log";
	        FileHandler fhandler = new FileHandler(pathLog, true);

	        try {

	            logger.addHandler(fhandler);
	            MyFormatter formatter = new MyFormatter();
	            fhandler.setFormatter(formatter);

	        } catch (SecurityException e) {
	            e.printStackTrace();
	            logger.info("error generating LOGS");
	        }

	    }
	 
	public static void get_estaciones_monitoreo() throws FileNotFoundException{
		
		    Logger logger = Logger.getLogger("etl_log");
	        List < estaciones > estaciones = new ArrayList < estaciones > ();        
	        Calendar fecha = Calendar.getInstance();			
	        String date_file = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", fecha);	        
	        
	        
	        PrintWriter pw= new PrintWriter(new File("C:\\datos\\estaciones\\estaciones_"+date_file+".csv"));
	        StringBuilder sb=new StringBuilder();	    
	        
	        String URL = "jdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_enlinea?noAccessToProcedureBodies=true&autoReconnect=true&useSSL=false";
	        String user = "gpcumpli_admin";
	        String pwd = "30cuY2[OAgAr";
	        
	       try {	    	   
	    	
	            Class.forName("com.mysql.jdbc.Driver");
	            Connection connection = DriverManager.getConnection(URL, user, pwd);
	            CallableStatement statement = connection.prepareCall("{call select_estaciones()}");
	            boolean hasResults = statement.execute();
	            if (hasResults) {
	                ResultSet rs = statement.getResultSet();
	                while (rs.next()) {
                        
	                	 sb.append(rs.getString(1));
	                	 sb.append("\r\n");
	                     estaciones.add(new estaciones(rs.getString(1)));

	                }
	                pw.write(sb.toString());
	                pw.close();
	            }	           
	            
	            statement.close();
	            System.out.println("Se ha obtenido la lista de estaciones");	           
	            connection.close();
	            
	        } catch (Exception e) {

	            System.out.println("error connection to GP database server :"+e);
	            logger.severe("error connection to GP database server : " + e);	            
	            SendMail.send_msg("error-etl", "error connection to GP (step 0) database server : " + e); 

	        }
	       
	        if(estaciones.size()>0)
	        {  
	        	
	        	  System.out.println("Searching entries for " + (estaciones.size()) + " tags..");
	        	  busqueda_lecturas(estaciones);       
	        }
	        else
	        {
	        	 logger.severe("No se lleno la lista de estaciones.. " );
	        	 System.out.println("No se lleno la lista de estaciones..");
	        }
	       

		
	}
	public static void busqueda_lecturas(List < estaciones > estaciones) throws FileNotFoundException{ 
		
		Logger logger = Logger.getLogger("etl_log");
		String dateInit   = Utils.get_date_start_today();
		String dateFinish =  Utils.get_date_now();
		int encountered = 0;
		List < lectura > lecturas = new ArrayList < lectura > ();		
		Calendar fecha = Calendar.getInstance();		
        String date_file = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", fecha);		
        PrintWriter pw= new PrintWriter(new File("C:\\datos\\muestras24h\\extraccion_"+date_file+".csv"));
        StringBuilder sb=new StringBuilder();
        
		/**************************************************************/		
        System.out.println("dateStart  :"+dateInit);
		System.out.println("dateFinish :"+dateFinish);
		System.out.println("Searching entries for " + (estaciones.size()) + " tags..");		
		/**************************************************************/
		
	    String URL = "jdbc:sqlserver://sqlsvrccazint01.database.windows.net:1433;";
	    String database = "databaseName=MLCCSMADATA;";
	    String user = "user=SMA_rd_GPConsultores@sqlsvrccazint01;";
	    String pwd = "password=czp5es?G_Y;";
	    String SQL;
	    
	    /**********************************************************/
	    
	    String connectionUrl = URL + database + user + pwd;	  
	    logger.info("searching entries > ="+ dateInit +" & < ="+ dateFinish );
	    
	    
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
    	 
    	 
    	 System.out.println(Utils.get_date_finish());
    	 System.out.println(Utils.get_date_now());
    	 System.out.println(Utils.get_date_start());
    	 System.out.println(Utils.get_date_start_today());
    	 

	    
	
	   try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection con = DriverManager.getConnection(connectionUrl);
            Statement stmt = con.createStatement();
            for (int i = 0; i <= estaciones.size() - 1; i++)
            {
                encountered = 0;
                /*Daemon 0*/
               // SQL = "select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura,VA from dbo.PozosSMA_GPConsultores  where TIMESTAMP >= '"+dateInit+"' AND  TIMESTAMP <=  '"+dateFinish+"' AND IDEstacion = '"+estaciones.get(i).nombre+"';";
                /*Daemon 1*/
              
                
                SQL = "select  IDEstacion, TIMESTAMP,Caudal,Conductividad,Nivel,PH,Temperatura,VA from dbo.PozosSMA_GPConsultores  where TIMESTAMP >= '"+Utils.get_date_start()+"' AND  TIMESTAMP <=  '"+Utils.get_date_start_today()+"' AND IDEstacion = '"+estaciones.get(i).nombre+"';";
                
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
               
                System.out.println("Searching entries for " +estaciones.get(i).nombre + " >" + Utils.get_date_start() + " & <"+ Utils.get_date_start_today()+" Process => " + Utils.percentage(i, estaciones.size() - 1) + " Result => " + encountered);
            }
            pw.write(sb.toString());
            pw.close();
            con.close();
            System.out.println("Connection with database SQL Server is closed");
        } catch (Exception e) {
        	
           logger.severe("error connecting to MLCC SQL SERVER " + e);
           System.out.println("error connecting to MLCC SQL SERVER " + e);           
           SendMail.send_msg("error-etl", "error connection to MLCC database server : " + e); 

        }
        logger.info("total entries encountered in mlcc database : " + lecturas.size());
        System.out.println("Total entries encountered : " + lecturas.size());
        
        if(lecturas.size()>0)
        {
        	  load_remote_server(lecturas);   	
        }
        else
        {
        	 logger.info("finish");
             System.out.println("finish");
        }

      
		
		
	}	
		
	 public static void load_remote_server(List < lectura > lecturas) {

	        Logger logger = Logger.getLogger("etl_log");   	     
	        String URL = "jdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_enlinea?noAccessToProcedureBodies=true&autoReconnect=true&useSSL=false";
	        String user = "gpcumpli_admin";
	        String pwd = "30cuY2[OAgAr";	        
	        int errores = 0;	        
	        
	        try {

	            Class.forName("com.mysql.jdbc.Driver");
	            Connection connection = DriverManager.getConnection(URL, user, pwd);
	            logger.info("transforming & sending data encountered to gpserver...");         
	            CallableStatement statement = connection.prepareCall("{call db_update(?, ?, ?, ?, ?, ? ,?, ?, ?)}");           
	      
	            for (int i = 0; i <= lecturas.size() - 1; i++) {
                        /***********************************************************************************************************************************/
		                /**/System.out.println("processing... " + i + " of " + (lecturas.size() - 1) + " tasks " + Utils.percentage(i, lecturas.size() - 1));              
		                /***********************************************************************************************************************************/
		                statement.setString(1, lecturas.get(i).estacion);
		                statement.setString(2, lecturas.get(i).time );
		                statement.setString(3, Utils.get_date_lector());
		                statement.setString(4, Utils.convert_2f(lecturas.get(i).ph));
		                statement.setString(5, Utils.convert_ce(lecturas.get(i).conductividad));
		                statement.setString(6, Utils.convert_2f(lecturas.get(i).temperatura));
		                statement.setString(7, Utils.convert_2f(lecturas.get(i).caudal));
		                statement.setString(8, Utils.convert_2f(lecturas.get(i).nivel));
		                statement.setString(9, Utils.convert_2f(lecturas.get(i).volumen));

	                try
	                {
	                    statement.execute();
	                    
	                } catch (SQLException e) {
	                   
	                	errores++;
	                    logger.severe("error :" + e);
	                    System.out.println("error :" + e);

	                }

	            }
	            
	            logger.info("Stored procedure called successfully!");
	            statement.close();
	            logger.info("connection with gp database is closed.. ");
	            System.out.println("Stored procedure called successfully!");
	            	            
	            if (errores != 0) {

	                logger.severe("data wasnt sent.. you have " + errores + " errors");

	            } else {

	                logger.info("data was sent successfully..");
	            }
	            logger.info("Bye ;) ");
	            System.out.println("errors : " + errores);
	            System.out.println("finish  : " + Utils.get_date_lector());

	        } catch (Exception e) {

	            e.printStackTrace();
	            System.out.println("Error connecting to gp database" + e);
	            logger.info("Error connecting to gp database " + e);
	            SendMail.send_msg("error-etl", "error connection to GP (step 2) database server : " + e); 
	            
	        }

	       SendMail.send_msg("extract-etl", "data was sent successfully.." ); 
	    }
	 
		 public static void show_data_server(List < lectura > lecturas)
		 {
		        for (int i = 0; i <= lecturas.size() - 1; i++)
		        {
	
		           System.out.println("estacion: " + lecturas.get(i).estacion + " fecha:" + lecturas.get(i).time + " caudal: " + lecturas.get(i).caudal + " " + "conductividad:" + Utils.convert_ce(lecturas.get(i).conductividad) + " " + "nivel :" + lecturas.get(i).nivel + "ph : " + lecturas.get(i).ph);
		            
		        }
	
		  }
	 	 


	
	 
	

}