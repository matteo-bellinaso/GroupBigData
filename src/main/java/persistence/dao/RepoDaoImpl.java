package persistence.dao;

import persistence.ConnectionProvider;
import persistence.dao.interfaces.RepoDao;
import persistence.entity.FileData;
import persistence.entity.Repo;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class RepoDaoImpl implements RepoDao {

    @Override
    public List<Repo> getAllRepo() {
        Connection conn = null;
        ResultSet result = null;
        Statement statement = null;
        List<Repo> repoList = null;
        try {
            conn = ConnectionProvider.openConnection();
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT * FROM project_tables.repo");

            repoList = fromRsToRepo(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, statement, conn);
        }

        return repoList;
    }

    @Override
    public Repo getRepoById(Long id) {
        Connection conn = null;
        ResultSet result = null;
        List<Repo> repoList = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("SELECT * FROM project_tables.repo where id_repo= ?");
            preparedStatement.setLong(1, id);
            result = preparedStatement.executeQuery();
            repoList = fromRsToRepo(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, preparedStatement, conn);
        }
        if (repoList != null && repoList.size() > 0) {
            return repoList.get(0);
        }

        return null;
    }
    @Override
    public boolean writeRepo(Repo repo) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = writeRepo(conn,repo);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
    }

    @Override
    public boolean writeRepos(List<Repo> repos) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            for (Repo repo : repos) {
                preparedStatement = writeRepo(conn, repo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);

    }

    @Override
    public boolean updateRepo(Repo repo) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("UPDATE project_tables.repo " +
                    "SET id_repo=?, name=?, url=? WHERE id_repo=?");
            preparedStatement.setObject(1, repo.getId());
            preparedStatement.setObject(2, repo.getName());
            preparedStatement.setObject(3, repo.getUrl());
            preparedStatement.setObject(4, repo.getId());

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
    public boolean deleteRepo(Long id) {
        Connection conn = null;
        int result = 0;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = utilityDeleteRepo(conn, id);
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
    public int deleteRepos(List<Long> listId) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            for (Long id : listId) {
                preparedStatement = utilityDeleteRepo(conn, id);
                result = preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
        }

        return result;
    }

    private PreparedStatement writeRepo(Connection conn, Repo repo) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO project_tables.repo (id_repo,name,url) VALUES (?,?,?)");
        preparedStatement.setObject(1, repo.getId());
        preparedStatement.setObject(2, repo.getName());
        preparedStatement.setObject(3, repo.getUrl());
        preparedStatement.execute();
        return preparedStatement;
    }


    private List<Repo> fromRsToRepo(ResultSet result) throws SQLException {
        List<Repo> repoList = new ArrayList<Repo>();
        Repo repo;
        while (result.next()) {
            repo = new Repo(result.getLong("id_repo"),
                    result.getString("name"),
                    result.getString("url"));
            repoList.add(repo);
        }
        return repoList;
    }

    private PreparedStatement utilityDeleteRepo(Connection conn, Long id) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("DELETE FROM project_tables.repo where id_repo= ?");
        preparedStatement.setLong(1, id);

        return preparedStatement;
    }
}

