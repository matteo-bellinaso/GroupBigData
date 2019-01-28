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

    private FileData sampleGenerator() {
        Long id = ((long) (Math.random() * 1000000));
        ids.add(id);
        return new FileData(id, new Actor(), new Repo(), "t", true, Date.valueOf("2018-01-01"));

    }

    private boolean writeOnDb(FileData fileData, boolean withRel) {
        return dao.writeFileData(fileData, withRel);
    }

    @AfterClass
    public static void delete() {
        dao.deleteFileDatas(ids);
    }

}