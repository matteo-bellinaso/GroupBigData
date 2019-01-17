package persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionProvider {
    public static Connection openConnection() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/projectDB", "postgres", "postgres");//
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    public static void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (SQLException exception) {
            System.out.println("Errore durante la chiusura della connessione: " + exception.getMessage());
        }
    }
}