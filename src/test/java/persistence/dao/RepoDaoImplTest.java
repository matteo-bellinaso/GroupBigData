package persistence.dao;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import persistence.dao.interfaces.RepoDao;
import persistence.entity.Repo;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class RepoDaoImplTest {
    static List<Long> ids;
    static RepoDao dao;

    @BeforeClass
    public static void initialize() {
        ids = new ArrayList<>();
        dao = new RepoDaoImpl();
    }

    @Test
    public void writeRepoTest() {
        assertTrue(writeOnDb(sampleGenerator()));
    }

    @Test
    public void readRepo() {
        writeOnDb(sampleGenerator());
        List<Repo> fileData = dao.getAllRepo();
        assertTrue(fileData.size() > 0);
    }

    @Test
    public void updateRepo() {
        Repo repo = sampleGenerator();
        writeOnDb(repo);
        Repo result = dao.getRepoById(repo.getId());
        assertEquals(repo, result);
        repo.setName("nameless");
        dao.updateRepo(repo);
        result = dao.getRepoById(repo.getId());
        assertEquals(repo, result);
    }

    private Repo sampleGenerator() {
        Long id = ((long) (Math.random() * 1000000));
        ids.add(id);
        return new Repo(id,"name","url");

    }

    private boolean writeOnDb(Repo repo) {
        return dao.writeRepo(repo);
    }

    @AfterClass
    public static void delete() {
        dao.deleteRepos(ids);
    }
}