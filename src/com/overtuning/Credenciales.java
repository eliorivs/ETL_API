package com.overtuning;

public class Credenciales {
	
	
	  class SQLSERVER{		  	
		  
			  static  final String URL = "jdbc:sqlserver://sqlsvrccazint01.database.windows.net:1433;";
			  static  final String database = "databaseName=MLCCSMADATA;";
			  static  final String user = "user=SMA_rd_GPConsultores@sqlsvrccazint01;";
			  static  final String pwd = "password=czp5es?G_Y;";
			  static  final String  clase ="com.microsoft.sqlserver.jdbc.SQLServerDriver";
	    }
      class MYSQL{
		  
		  
			  static final  String URL = "jdbc:mysql://gpcumplimiento.cl:3306/gpcumpli_bombeosld?noAccessToProcedureBodies=true&autoReconnect=true&useSSL=false";
			  static final  String user = "gpcumpli_admin";
			  static final  String pwd = "30cuY2[OAgAr";		  
			  static final  String clase="com.mysql.jdbc.Driver";
	   }
      class ENPOINTS {
    	  
    	  static final  String json ="https://gpconsultores.cl/PDC_ONLINE/backend/ultimas_lecturas.php";
    	  static final  String post ="https://gpconsultores.cl/PDC_ONLINE/backend/post.php";
    	  
    	  
      }

}
