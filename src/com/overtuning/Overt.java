package com.overtuning;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.json.*; 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
 


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
	        private String time;

	        private estaciones(String _Nombre, String _Time ) {

	            nombre = _Nombre;
	            time =_Time;
	            

	        }

	    }
	  
	  public static class notificaciones {

	        private String id;
	        private String time;

	        private notificaciones(String _Id, String _Time ) {

	            id = _Id;
	            time =_Time;
	            

	        }

	    }
	  
	 
	  
	 
	public static void main(String[] args) throws SecurityException, IOException ,JSONException {
		// TODO Auto-generated method stub
		
	     
		 System.out.println("Script for load entries... CAT");		 
		 configure_logs();
		 url_update();
		 /* try {
			  ComunicationWS();
	        } catch (IOException e) {
	            System.out.println(e.getMessage());
	        }*/
		
	   
		
	}
	
	
	private static void url_update() throws IOException, JSONException
	{ 
		Logger logger = Logger.getLogger("etl_log");
		List < estaciones > estaciones = new ArrayList < estaciones > ();        
	    Calendar fecha = Calendar.getInstance();			
	    String date_file = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", fecha);	    
		 
		 logger.info("Starting ...");
		 try {
			    System.out.println("Connecting to Web Service...");
			    logger.info("Connecting to web service...");			
			    String url =Credenciales.ENPOINTS.json;
			    logger.info("estableciendo conexion con "+ url);
			    JSONObject json = readJsonFromUrl(url);
			    JSONArray jsonArray =  json.getJSONArray("lecturas");	    
			    for(int i=0;i<jsonArray.length();i++)
			    {
			    	
			    	 JSONObject jsonObject = jsonArray.getJSONObject(i);
			    	 String estacion = jsonObject.getString("estacion");
			    	 String ultimalectura = jsonObject.getString("timestamp");
			    	 System.out.println("Desde API:"+estacion + " "+ultimalectura);
			    	 
			    	 estaciones.add(new estaciones(estacion,ultimalectura));
			    	 		    	
			    	 
			    	 
			    	
			    }
			    logger.info("lista de estaciones obtenida desde "+url);
			    
			  
		    } catch (Exception e){
		        
		    	System.out.println("Imposible resolver URL"+e);
		    	logger.severe("Imposible resolver URL "+e);
		    }
		 
		 
		    if(estaciones.size()>0)
	        {  
	        	
	        	
			 System.out.println("here we go!");
			 logger.severe("econtradas "+ estaciones.size() +" rows via api ");
			 busqueda_lecturas(estaciones);
			 
	        	  
	        }
	        else
	        {
	        	 logger.severe("No se lleno la lista de estaciones.. " );
	        	 System.out.println("No se lleno la lista de estaciones..");
	        }
		 
		 
		 
	}
	
	
	
	
	 private static void configure_logs() throws SecurityException, IOException {

	        boolean append = false;
	        Logger logger = Logger.getLogger("etl_log");
	        logger.setUseParentHandlers(append);
	        String pathLog = "C:/datos/daemon_logs_update_API.log";
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
	 
	public static void get_estaciones_monitoreo()  throws FileNotFoundException {
		
		    Logger logger = Logger.getLogger("etl_log");
	        List < estaciones > estaciones = new ArrayList < estaciones > ();        
	        Calendar fecha = Calendar.getInstance();			
	        String date_file = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", fecha);	                
	        PrintWriter pw= new PrintWriter(new File("C:\\datos\\estaciones\\estaciones_"+date_file+"_bombeo_ld.csv"));
	        StringBuilder sb=new StringBuilder();          
	   	    
	       try {	    	   
	    	
	            Class.forName(Credenciales.MYSQL.clase);
	            Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL, Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
	            CallableStatement statement = connection.prepareCall("{call ultimas_lecturas()}");
	            boolean hasResults = statement.execute();
	            if (hasResults) {
	                ResultSet rs = statement.getResultSet();
	                while (rs.next())
	                {
                        
	                	 sb.append(rs.getString(1));
	                	 sb.append("\r\n");
	                     estaciones.add(new estaciones(rs.getString(2),rs.getString(3)));

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
	        	
	        	  //System.out.println("Searching entries for " + (estaciones.size()) + " tags..");	 */       	
	        	  //busqueda_lecturas(estaciones);
	        	  
	        }
	        else
	        {
	        	 logger.severe("No se lleno la lista de estaciones.. " );
	        	 System.out.println("No se lleno la lista de estaciones..");
	        }
	       

		
	}
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
        	 load_remote_server(lecturas); 
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
	public static void eliminar_notificaciones(){
		
		    Logger logger = Logger.getLogger("etl_log"); 		   
	        String SQL;        
	        
	          try{
	        	   Class.forName(Credenciales.MYSQL.clase);	
	        	   Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL,Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
		           Statement stmt = connection.createStatement();
		           SQL = "delete from notificaciones";
		           int rs = stmt.executeUpdate(SQL);
	             }
	             catch (Exception e)
	             {
	            	 System.out.println("error connecting to MLCC SQL SERVER " + e);    
	             }
			     logger.info("Eliminadas Notificaciones");
		
	}
	public static void crear_notificacion()
	{		
		    Logger logger = Logger.getLogger("etl_log"); 
		    String SQL;
	        
	        
	          try{
	        	   Class.forName(Credenciales.MYSQL.clase);
	        	   Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL,Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
		           Statement stmt = connection.createStatement();
		           SQL = " insert into notificaciones (notificacion_time) values ('"+Utils.get_date_now()+"')";
		           int rs = stmt.executeUpdate(SQL);
	             }
	             catch (Exception e)
	             {
	            	 System.out.println("error al intentar grabar notificacion " + e);
	            	 logger.info("error al intentar grabar notificacion " + e);
	             }
	            logger.info("Notificacion");
	  
	        
		
	}
	public static void  buscar_notificaciones() {
		    Logger logger = Logger.getLogger("etl_log");
		    List < notificaciones > notificaciones = new ArrayList < notificaciones > (); 		
	        String SQL;

	    
	        int encountered=0;
	        
	        
	          try{
		        	   Class.forName(Credenciales.MYSQL.clase);		      
			           Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL,Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
			           Statement stmt = connection.createStatement();
			           SQL = "select * from notificaciones";
			           ResultSet rs = stmt.executeQuery(SQL);
			           while (rs.next())
		               {
			        	    notificaciones.add(new notificaciones(rs.getString(1), rs.getString(2)));                   
		                    encountered++;
		               }
			           if(encountered>0)
			           {
			        	   System.out.println("notificacion activa desde las "+ notificaciones.get(0).time); 
			        	   logger.info("notificacion activa desde las "+ notificaciones.get(0).time);
			        	   
			        	   returminutes(notificaciones.get(0).time);
			           }
			           if(encountered==0)
			           {
			        	   System.out.println("Activando notificacion en BD");
			        	   logger.info("Activando notificacion en BD");
			        	   crear_notificacion();
			        
			           }
		               
	              }
	             catch (Exception e)
	             {
	            	 System.out.println("error al intentar obtener notificacion " + e);
	            	 logger.info("error al intentar obtener notificacion " + e);
	             }		
		
	}
	
	public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
	    return new java.sql.Date(date.getTime());
	}
	
	public static void returminutes(String timedb) throws ParseException {
		
		 Logger logger = Logger.getLogger("etl_log");
		 java.util.Date date1=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timedb);
		 java.util.Date date2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(Utils.get_date_now());
		 System.out.println(date1);
		 System.out.println(date2);		 
		 long milliseconds = date2.getTime()-date1.getTime();		 
		 long minutes = (milliseconds / 1000) / 60;		 
		 System.out.println("Alarma activa hace..."+minutes +" minutos");
		 if(minutes>120)
		 {
			 SendMail.send_msg("Alarma - Monitoreo en Línea - MLCC ", "No se han volcado datos desde hace "+minutes+" minutos en el servidor de Monitoreo. \nPlease, DO NOT answer this message, it is an automatic sending");  
			 logger.info("Alarma - Monitoreo en Línea - MLCC" );
					
		 }
		 else
		 {
			 System.out.println("Alarma - Monitoreo en Línea - MLCC [email no sent]");
			 logger.info("Alarma - Monitoreo en Línea - MLCC [email no sent]" );
			
		 }	

		
	}
	public static void loadJsonServer(String estacion) throws JSONException, IOException
	{
		//JSONObject params = new JSONObject();
    	
		//System.out.print(params);
		
		 try
		 {
			 
		 }
		 catch (Exception e)
         {
			 
         }
		 
		
		URL url = new URL("https://gpconsultores.cl/PDC_ONLINE/backend/post.php"); // URL to your application
	    Map<String,Object> params = new LinkedHashMap<String, Object>();
	    params.put("value", 5); // All parameters, also easy
	    params.put("id", 17);
	    params.put("estacion", estacion);
	    StringBuilder postData = new StringBuilder();
	    // POST as urlencoded is basically key-value pairs, as with GET
	    // This creates key=value&key=value&... pairs
	    for (Map.Entry<String,Object> param : params.entrySet()) {
	        if (postData.length() != 0) postData.append('&');
	        postData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
	        postData.append('=');
	        postData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
	    }

	    // Convert string to byte array, as it should be sent
	    byte[] postDataBytes = postData.toString().getBytes("UTF-8");

	    // Connect, easy
	    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	    // Tell server that this is POST and in which format is the data
	    conn.setRequestMethod("POST");
	    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
	    conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));
	    conn.setDoOutput(true);
	    conn.getOutputStream().write(postDataBytes);

	    // This gets the output from your server
	    Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

	    for (int c; (c = in.read()) >= 0;)
	        System.out.print((char)c);
	}
	
	public static void ComunicationWS(String estacion, String MachineTime,String pH, String Timestamp ,String Conductividad, String Temperatura, String Caudal, String Nivel, String Volumen) throws IOException
	{
		    Logger logger = Logger.getLogger("etl_log");   
			URL url = new URL("https://gpconsultores.cl/PDC_ONLINE/backend/post.php");
			
			
	        Map<String, Object> params = new LinkedHashMap<String, Object>();
	        params.put("estacion", estacion);
	        params.put("horaMaquina", MachineTime);
	        params.put("pH", pH);
	        params.put("timestamp", Timestamp);
	        params.put("conductividad", Conductividad);
	        params.put("temperatura", Temperatura);
	        params.put("caudal", Caudal);
	        params.put("volumen", Volumen);
	        params.put("nivel", Nivel);	
	        StringBuilder postData = new StringBuilder();
	        for (Map.Entry<String, Object> param : params.entrySet()) {
	            if (postData.length() != 0)
	                postData.append('&');
	            postData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
	            postData.append('=');
	            postData.append(URLEncoder.encode(String.valueOf(param.getValue()),
	                    "UTF-8"));
	        }
	        byte[] postDataBytes = postData.toString().getBytes("UTF-8");	 
	        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Content-Type",
	                "application/x-www-form-urlencoded");
	        conn.setRequestProperty("Content-Length",
	                String.valueOf(postDataBytes.length));
	        conn.setDoOutput(true);
	        conn.getOutputStream().write(postDataBytes);
	 
	        Reader in = new BufferedReader(new InputStreamReader(
	                conn.getInputStream(), "UTF-8"));
	        for (int c = in.read(); c != -1; c = in.read())
	            System.out.print((char) c);
	        
	        System.out.print("\n");
	}
		
	 public static void load_remote_server(List < lectura > lecturas) throws JSONException, IOException {
		
	        Logger logger = Logger.getLogger("etl_log");   
	       	int errores = 0;	
	       	URL url = new URL("https://gpconsultores.cl/PDC_ONLINE/backend/post.php");
	       	System.out.println("enviando a WebService "+url);
	        logger.info("Enviado a WebService..." );
	        for (int i = 0; i <= lecturas.size() - 1; i++)
	        {
	        	 
	        	
	        	 ComunicationWS(lecturas.get(i).estacion,Utils.get_date_lector(),lecturas.get(i).ph,lecturas.get(i).time,lecturas.get(i).conductividad,lecturas.get(i).temperatura,lecturas.get(i).caudal,lecturas.get(i).nivel,lecturas.get(i).volumen);
	        }
	   	    System.out.println("Finish");
	   	    logger.info("End.... See you later.");	       	
	       	
	    }
	 
		 public static void show_data_server(List < lectura > lecturas)
		 {
		        for (int i = 0; i <= lecturas.size() - 1; i++)
		        {
	
		           System.out.println("estacion: " + lecturas.get(i).estacion + " fecha:" + lecturas.get(i).time + " caudal: " + lecturas.get(i).caudal + " " + "conductividad:" + Utils.convert_ce(lecturas.get(i).conductividad) + " " + "nivel :" + lecturas.get(i).nivel + "ph : " + lecturas.get(i).ph);
		            
		        }
	
		  }
		 public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {

	          InputStream is = new URL(url).openStream();
	        try {
	          BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	          String jsonText = readAll(rd);
	          JSONObject json = new JSONObject(jsonText);
	          return json;
	        } finally {
	          is.close();
	        }
	      }
		 private static String readAll(Reader rd) throws IOException {
		        StringBuilder sb = new StringBuilder();
		        int cp;
		        while ((cp = rd.read()) != -1) {
		          sb.append((char) cp);
		        }
		        return sb.toString();
		      }
	 	 


	
	 
	

}