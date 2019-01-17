package persistence.dao;

import persistence.dao.interfaces.RepoDao;
import persistence.entity.Repo;

import java.util.List;

public class RepoDaoImpl implements RepoDao {
    public List<Repo> getAllRepo() {
        return null;
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
}
