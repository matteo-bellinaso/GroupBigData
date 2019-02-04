package persistence;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConnectionProviderTest extends DatabasePreparator{

    @Test
    public void openConnection() {
        ConnectionProvider.openConnection();
        assertNotNull(ConnectionProvider.openConnection());
    }
}