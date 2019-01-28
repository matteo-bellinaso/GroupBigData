package persistence.dao.interfaces;

import persistence.entity.Actor;

import java.util.List;

public interface ActorDao {
    List<Actor> getAllActor();

    Actor getActorById(Long id);

    boolean writeActor(Actor actor);

    boolean writeActors(List<Actor> actors);

    boolean updateActor(Actor actor);

    boolean deleteActor(Long id);

    int deleteActors(List<Long> id);
}
