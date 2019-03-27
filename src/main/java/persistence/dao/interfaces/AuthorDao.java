package persistence.dao.interfaces;

import persistence.entity.Author;

import java.util.List;

public interface AuthorDao {
    List<Author> getAllAuthor();

    Author getAuthorByMail(String email);

    boolean writeAuthor(Author author);

    boolean writeAuthors(List<Author> authors);

    boolean updateAuthor(Author actor);

    boolean deleteAuthor(String email);

    int deleteAuthors(List<String> emails);
}