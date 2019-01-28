package persistence.dao.interfaces;

import persistence.entity.Actor;
import persistence.entity.Repo;

import java.util.List;

public interface RepoDao {
    List<Repo> getAllRepo();

    Repo getRepoById(Long id);

    boolean writeRepo(Repo repo);

    boolean writeRepos(List<Repo> repos);

    boolean updateRepo(Repo repo);

    boolean deleteRepo(Long id);

    int deleteRepos(List<Long> listId);
}
