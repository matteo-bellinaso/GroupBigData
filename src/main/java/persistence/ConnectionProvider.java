package persistence;



import entity.ConnectionScala;
import properties.ApplicationConfig;

import java.sql.*;

public class ConnectionProvider {


    public static Connection openConnection() {


        ConnectionScala connectionScala = ApplicationConfig.instance().setConnectionConfiguration();

        Connection conn = null;
        try {
            Class.forName(connectionScala.driver());
            conn = DriverManager.getConnection(connectionScala.url(), connectionScala.user(), connectionScala.password());
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    public static void closeResultSetAndStatementAndConnection(ResultSet result, Statement statement, Connection conn) {
       ConnectionProvider.closeResultSetAndStatent(result,statement);
        ConnectionProvider.closeConnection(conn);
    }

    public static  void closeResultSetAndStatent(ResultSet result, Statement statement){
        try {
            if(result!=null) {
                result.close();
            }
        } catch (Exception rse) {
            rse.printStackTrace();
        }
        try {
            if(statement!= null) {
                statement.close();
            }
        } catch (Exception sse) {
            sse.printStackTrace();
        }
    }

    public static boolean closeStatementAndConnection(Statement statement, Connection conn) {
        try {
            if(statement!= null) {
                statement.close();
            }
        } catch (Exception sse) {
            sse.printStackTrace();
            return false;
        }
        return ConnectionProvider.closeConnection(conn);
    }

    private static boolean closeConnection(Connection conn) {
        try {
            if(conn != null) {
                conn.close();
            }
            return true;
        } catch (SQLException exception) {
            System.out.println("Errore durante la chiusura della connessione: " + exception.getMessage());
            return false;
        }
    }


}