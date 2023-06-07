package com.overtuning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import com.overtuning.Overt.estaciones;
import com.overtuning.Overt.lectura;
import com.overtuning.Serialize.Serials;






public class Transaccion {
	
	
	
	
	
	
	public static void loadJsonServer(String estacion) throws JSONException, IOException
	{
		
		 
		
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
	
	
	public static void url_update() throws IOException, JSONException
	{ 
		Logger logger = Logger.getLogger("etl_log");
		List < estaciones > estaciones = new ArrayList < estaciones > ();        
	    Calendar fecha = Calendar.getInstance();			
	    String date_file = String.format("%1$tY%1$tm%1$td_%1$tH%1$tM%1$tS", fecha);	    
		 
		 logger.info("Starting ...");
		 try {
			    System.out.println("Connecting to Web Service...");
			    logger.info("Connecting to web service...");			
			    String url =Credenciales.ENDPOINTS.json;
			    logger.info("estableciendo conexion con "+ url);
			    JSONObject json = Overt.readJsonFromUrl(url);			
			    JSONArray jsonArray =  json.getJSONArray("lecturas");	    
			    for(int i=0;i<jsonArray.length();i++)
			    {
			    	
			    	 JSONObject jsonObject = jsonArray.getJSONObject(i);
			    	 String estacion = jsonObject.getString("estacion");
			    	 String ultimalectura = jsonObject.getString("timestamp");
			    	 System.out.println("Desde API:"+estacion + " "+ultimalectura);			    	 
			    	 estaciones.add(new estaciones(estacion,ultimalectura));
			    	 
			    }
			
			    
			  
		    } catch (Exception e){
		        
		    	System.out.println("Imposible resolver URL"+e);
		    	logger.severe("Imposible resolver URL "+e);
		    }
		 
		 
		    if(estaciones.size()>0)
	        { 
	        
		        	
				 System.out.println("here we go!");
				 System.out.println("econtradas "+ estaciones.size() +" rows via api ");
				 logger.severe("econtradas "+ estaciones.size() +" rows via api ");
				 Extraccion.busqueda_lecturas(estaciones);				
			 
	        	  
	        }
	        else
	        {
	        	 logger.severe("No se lleno la lista de estaciones.. " );
	        	 System.out.println("No se lleno la lista de estaciones..");
	        }
		 
		 
		 
	}
	
	public static String ifNull(String variable)
	{
	    if(variable == null)
	    {
	        variable ="";
	    	
	    }	  
	    return variable;
	    
	   
	}
	
	
	public static  void GenerateJson(String estacion, String MachineTime,String pH, String Timestamp ,String Conductividad, String Temperatura, String Caudal, String Nivel, String Volumen) throws IOException
	{
		StringBuilder postDataBuilder = new StringBuilder();
        postDataBuilder.append("{");
        postDataBuilder.append("\"estacion\":\"" + estacion + "\",");
        postDataBuilder.append("\"horaMaquina\":\"" + MachineTime + "\",");
        postDataBuilder.append("\"timestamp\":\"" + Timestamp+ "\",");
        postDataBuilder.append("\"pH\":\"" + ifNull(pH) + "\",");
        postDataBuilder.append("\"conductividad\":\"" + ifNull(Conductividad) + "\",");
        postDataBuilder.append("\"temperatura\":\"" + ifNull(Temperatura) + "\",");
        postDataBuilder.append("\"caudal\":\"" + ifNull(Caudal) + "\",");
        postDataBuilder.append("\"nivel\":\"" + ifNull(Nivel)+ "\",");
        postDataBuilder.append("\"volumen\":\"" + ifNull(Volumen) + "\"");
        postDataBuilder.append("}");        
        String postData = postDataBuilder.toString();
        System.out.println("Posteando =>");
        System.out.println(postData);
        PostearJson(postData);        
		
	}
	public static  void PostearJson(String postData) throws IOException
	{
		String URL =  Credenciales.ENDPOINTS.post;
		URL obj = new URL(URL);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);       
  
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.write(postData.getBytes(StandardCharsets.UTF_8));
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();  
        System.out.println("Respuesta del servidor:");
        System.out.println(response.toString());
		
		
	}
	
	
	public static void ComunicationWS(String estacion, String MachineTime,String pH, String Timestamp ,String Conductividad, String Temperatura, String Caudal, String Nivel, String Volumen) throws IOException
	{
		    Logger logger = Logger.getLogger("etl_log");  
			URL url = new URL(Credenciales.ENDPOINTS.post);				
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
	       	URL url = new URL(Credenciales.ENDPOINTS.post);
	       	System.out.println("enviando a WebService "+url);
	        logger.info("Enviado a WebService..." );
	        for (int i = 0; i <= lecturas.size() - 1; i++)
	        {
	        	GenerateJson(lecturas.get(i).estacion,Utils.get_date_lector(),lecturas.get(i).ph,lecturas.get(i).time,lecturas.get(i).conductividad,lecturas.get(i).temperatura,lecturas.get(i).caudal,lecturas.get(i).nivel,lecturas.get(i).volumen);
	        }
	   	    System.out.println("Finish");
	   	    logger.info("End.... See you later.");	       	
	       	
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

}
