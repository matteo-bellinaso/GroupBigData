package persistence.dao;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import persistence.DatabasePreparator;
import persistence.dao.interfaces.AuthorDao;
import persistence.entity.Author;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthorDaoImplTest extends DatabasePreparator {
    static AuthorDao dao;
    static List<String> mails;

    @BeforeClass
    public static void initialize() {
        mails= new ArrayList<>();
        dao = new AuthorDaoImpl();
    }

    @Test
    public void writeAuthorTest() {
        assertTrue(writeOnDb(sampleGenerator()));
    }

    @Test
    public void readAuthor() {
        writeOnDb(sampleGenerator());
        List<Author> fileData = dao.getAllAuthor();
        assertTrue(fileData.size() > 0);
    }

    @Test
    public void updateAuthor() {
        Author author = sampleGenerator();
        writeOnDb(author);
        Author result = dao.getAuthorByMail(author.getEmail());
        assertEquals(author, result);
        author.setName("nuovo nome");
        dao.updateAuthor(author);
        result = dao.getAuthorByMail(author.getEmail());
        assertEquals(author, result);
    }

    private Author sampleGenerator() {
        String mail = "mail@"+((long) (Math.random() * 1000000))+".com";
        mails.add(mail);
        return new Author("nome",mail);

    }

    private boolean writeOnDb(Author author) {
        return dao.writeAuthor(author);
    }

    @AfterClass
    public static void delete() {
        dao.deleteAuthors(mails);
    }
}