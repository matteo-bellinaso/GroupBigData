package persistence.dao.interfaces;

import persistence.entity.Repo;

import java.util.List;

public interface RepoDao {
    List<Repo> getAllRepo();

    Repo getRepoById(Long id);

    boolean writeRepo(Repo repo);

    boolean writeRepos(List<Repo> repos);
}
