package persistence;

import org.junit.BeforeClass;
import properties.ApplicationConfig;
import utility.Paths;

import java.util.ArrayList;
import java.util.List;


public class DatabasePreparator {

    protected static List<Long> ids;

    @BeforeClass
    public static void loadProperties(){
        ApplicationConfig.init(Paths.testConfigPath);
        ids = new ArrayList<>();
    }
}
