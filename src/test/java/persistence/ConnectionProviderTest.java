package persistence;

import static org.junit.Assert.*;

public class ConnectionProviderTest {

    @org.junit.Test
    public void openConnection() {
        ConnectionProvider.openConnection();
        assertNotNull(ConnectionProvider.openConnection());
    }
}