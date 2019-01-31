package persistence.dao;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import persistence.dao.interfaces.FileDataDao;
import persistence.entity.Actor;
import persistence.entity.FileData;
import persistence.entity.Repo;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FileDataDaoImplTest {
    private Date sampleDate=Date.valueOf("2018-01-01");
    private Date sampleDate2=Date.valueOf("2018-01-01");
    static List<Long> ids;
    static FileDataDao dao;

    @BeforeClass
    public static void initialize() {
        ids = new ArrayList<>();
        dao = new FileDataDaoImpl();
    }

    @Test
    public void writeFileDataTest() {
        assertTrue(writeOnDb(sampleGenerator(), false));
    }

    @Test
    public void readFileData() {
        writeOnDb(sampleGenerator(), false);
        List<FileData> fileData = dao.getAllFileData(false);
        assertTrue(fileData.size() > 0);
    }

    @Test
    public void updateFileData() {
        FileData fileData = sampleGenerator();
        writeOnDb(fileData, false);
        FileData result = dao.getFileDataById(fileData.getId_file(), false);
        assertEquals(fileData, result);
        fileData.setPubblico(false);
        fileData.setCreated_at(sampleDate2);
        dao.updateFileData(fileData, false);
        result = dao.getFileDataById(fileData.getId_file(), false);
        assertEquals(fileData, result);
    }


    @Test
    public void selectRelationatedFile() {
        delete();
        ids = new ArrayList<>();
        FileData fileData = sampleGeneratorWithRel();
        assertTrue(writeOnDb(fileData, true));
        List<FileData> result = dao.getFileDatabyCreationDate(sampleDate, true);
        assertEquals(fileData.getActor(),result.get(0).getActor());
        assertEquals(fileData.getRepo(),result.get(0).getRepo());
        dao.deleteFileDatas(ids,true);

    }

    private FileData sampleGenerator() {
        Long id = ((long) (Math.random() * 1000000));
        ids.add(id);
        return new FileData(id, new Actor(), new Repo(), "t", true, sampleDate);

    }

    private FileData sampleGeneratorWithRel() {
        Long id = ((long) (Math.random() * 1000000));
        Actor actor = new Actor(id, "login", "display_login", "gravatar_id", "url", "avatar_url");
        id = ((long) (Math.random() * 1000000));
        Repo repo = new Repo(id, "name", "url");
        id = ((long) (Math.random() * 1000000));
        ids.add(id);
        return new FileData(id, actor, repo, "t", true, sampleDate);
    }

    private boolean writeOnDb(FileData fileData, boolean withRel) {
        return dao.writeFileData(fileData, withRel);
    }

    @AfterClass
    public static void delete() {
        dao.deleteFileDatas(ids,true);
    }

}