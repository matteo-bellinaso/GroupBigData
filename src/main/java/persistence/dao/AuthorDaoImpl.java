package persistence.dao;

import persistence.ConnectionProvider;
import persistence.dao.interfaces.AuthorDao;
import persistence.entity.Author;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AuthorDaoImpl  implements AuthorDao {

    public List<Author> getAllAuthor() {
        Connection conn = null;
        ResultSet result = null;
        Statement statement = null;
        List<Author> authorList = null;
        try {
            conn = ConnectionProvider.openConnection();
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT * FROM project_tables.author");

            authorList = fromRsToAuthor(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, statement, conn);
        }

        return authorList;
    }

    @Override
    public Author getAuthorByMail(String email) {
        Connection conn = null;
        ResultSet result = null;
        List<Author> authorList = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("SELECT * FROM project_tables.author where email= ?");
            preparedStatement.setString(1, email);
            result = preparedStatement.executeQuery();
            authorList = fromRsToAuthor(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, preparedStatement, conn);
        }
        if (authorList != null && authorList.size() > 0) {
            return authorList.get(0);
        }

        return null;
    }

    @Override
    public boolean writeAuthor(Author author) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = writeAuthor(conn, author);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);

    }

    @Override
    public boolean writeAuthors(List<Author> authors) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            for (Author author : authors) {
                preparedStatement = writeAuthor(conn, author);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);

    }

    @Override
    public boolean updateAuthor(Author author) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("UPDATE project_tables.author SET name=?, email=? WHERE email=?");

            preparedStatement.setObject(1, author.getName());
            preparedStatement.setObject(2, author.getEmail());
            preparedStatement.setObject(3, author.getEmail());
            result = preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
        }
        if (result == 1) {
            return true;
        }
        return false;
    }

    @Override
    public boolean deleteAuthor(String email) {
        Connection conn = null;
        int result = 0;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = utilityDeleteAuthor(conn, email);
            result = preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
        }
        if (result == 1) {
            return true;
        }

        return false;
    }

    @Override
    public int deleteAuthors(List<String> emails) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            for (String email : emails) {
                preparedStatement = utilityDeleteAuthor(conn, email);
                result = preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
        }

        return result;
    }

    private PreparedStatement writeAuthor(Connection conn, Author author) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO project_tables.author (name,email) VALUES (?,?)");

        preparedStatement.setObject(1, author.getName());
        preparedStatement.setObject(2, author.getEmail());
        preparedStatement.execute();
        return preparedStatement;
    }

    private PreparedStatement utilityDeleteAuthor(Connection conn, String email) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("DELETE FROM project_tables.author where email= ?");
        preparedStatement.setString(1, email);

        return preparedStatement;
    }

    private List<Author> fromRsToAuthor(ResultSet result) throws SQLException {
        List<Author> authorList = new ArrayList<Author>();
        Author author;
        while (result.next()) {

            author = new Author(result.getString("name"),
                    result.getString("email"));
            authorList.add(author);
        }
        return authorList;
    }
}
