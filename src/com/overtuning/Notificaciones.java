package com.overtuning;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.overtuning.Overt.notificaciones;

public class Notificaciones {

    public static void eliminar_notificaciones() {

        Logger logger = Logger.getLogger("etl_log");
        String SQL;

        try {
            Class.forName(Credenciales.MYSQL.clase);
            Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL, Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
            Statement stmt = connection.createStatement();
            SQL = "delete from notificaciones";
            int rs = stmt.executeUpdate(SQL);
        } catch (Exception e) {
            System.out.println("error connecting to MLCC SQL SERVER " + e);
        }
        logger.info("Eliminadas Notificaciones");

    }

    public static void crear_notificacion() {
        Logger logger = Logger.getLogger("etl_log");
        String SQL;


        try {
            Class.forName(Credenciales.MYSQL.clase);
            Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL, Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
            Statement stmt = connection.createStatement();
            SQL = " insert into notificaciones (notificacion_time) values ('" + Utils.get_date_now() + "')";
            int rs = stmt.executeUpdate(SQL);
        } catch (Exception e) {
            System.out.println("error al intentar grabar notificacion " + e);
            logger.info("error al intentar grabar notificacion " + e);
        }
        logger.info("Notificacion");



    }
    public static void buscar_notificaciones() {
        Logger logger = Logger.getLogger("etl_log");
        List < notificaciones > notificaciones = new ArrayList < notificaciones > ();
        String SQL;


        int encountered = 0;


        try {
            Class.forName(Credenciales.MYSQL.clase);
            Connection connection = DriverManager.getConnection(Credenciales.MYSQL.URL, Credenciales.MYSQL.user, Credenciales.MYSQL.pwd);
            Statement stmt = connection.createStatement();
            SQL = "select * from notificaciones";
            ResultSet rs = stmt.executeQuery(SQL);
            while (rs.next()) {
                notificaciones.add(new notificaciones(rs.getString(1), rs.getString(2)));
                encountered++;
            }
            if (encountered > 0) {
                System.out.println("notificacion activa desde las " + notificaciones.get(0).time);
                logger.info("notificacion activa desde las " + notificaciones.get(0).time);

                returminutes(notificaciones.get(0).time);
            }
            if (encountered == 0) {
                System.out.println("Activando notificacion en BD");
                logger.info("Activando notificacion en BD");
                crear_notificacion();

            }

        } catch (Exception e) {
            System.out.println("error al intentar obtener notificacion " + e);
            logger.info("error al intentar obtener notificacion " + e);
        }

    }
    public static void returminutes(String timedb) throws ParseException {

        Logger logger = Logger.getLogger("etl_log");
        java.util.Date date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timedb);
        java.util.Date date2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(Utils.get_date_now());
        System.out.println(date1);
        System.out.println(date2);
        long milliseconds = date2.getTime() - date1.getTime();
        long minutes = (milliseconds / 1000) / 60;
        System.out.println("Alarma activa hace..." + minutes + " minutos");
        if (minutes > 120) {
            SendMail.send_msg("Alarma - Monitoreo en L�nea - MLCC ", "No se han volcado datos desde hace " + minutes + " minutos en el servidor de Monitoreo. \nPlease, DO NOT answer this message, it is an automatic sending");
            logger.info("Alarma - Monitoreo en L�nea - MLCC");

        } else {
            System.out.println("Alarma - Monitoreo en L�nea - MLCC [email no sent]");
            logger.info("Alarma - Monitoreo en L�nea - MLCC [email no sent]");

        }


    }


}