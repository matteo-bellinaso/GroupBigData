package persistence.dao;

import persistence.ConnectionProvider;
import persistence.dao.interfaces.RepoDao;
import persistence.entity.Repo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class RepoDaoImpl implements RepoDao {
    public List<Repo> getAllRepo() {
        Connection conn = null;
        ResultSet result = null;
        Statement statement = null;
        List<Repo> repoList = null;
        try {
            conn = ConnectionProvider.openConnection();
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT * FROM repo");

            repoList = fromRsToRepo(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result,statement,conn);
        }

        return repoList;
    }

    public Repo getRepoById(Long id) {
        return null;
    }

    public boolean writeRepo(Repo repo) {
        return false;
    }

    public boolean writeRepos(List<Repo> repos) {
        return false;
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
}

