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

        public String estacion;
        public String time;
        public String caudal;
        public String conductividad;
        public String nivel;
        public String ph;
        public String temperatura;
        public String volumen;

        public lectura(String _Estacion, String _Time, String _Caudal, String _Conductividad, String _Nivel, String _PH, String _Temperatura, String _Volumen) {

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

        public String nombre;
        public String time;

        public estaciones(String _Nombre, String _Time) {

            nombre = _Nombre;
            time = _Time;


        }

    }
    public static class notificaciones {

        public String id;
        public String time;

        public notificaciones(String _Id, String _Time) {

            id = _Id;
            time = _Time;


        }

    }
    public static void main(String[] args) throws SecurityException, IOException, JSONException {
        // TODO Auto-generated method stub

        try {
            System.out.println("Startind process.. please wait..");
            configure_logs();
            Transaccion.url_update();
        } catch (SecurityException e) {
            e.printStackTrace();

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



    public static void show_data_server(List < lectura > lecturas) {
        for (int i = 0; i <= lecturas.size() - 1; i++) {

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