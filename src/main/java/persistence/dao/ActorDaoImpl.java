package persistence.dao;

import persistence.dao.interfaces.ActorDao;
import persistence.entity.Actor;

import java.util.List;

public class ActorDaoImpl implements ActorDao {
    public List<Actor> getAllActor() {
        return null;
    }

    public Actor getActorById(Long id) {
        return null;
    }

    public boolean writeActor(Actor actor) {
        return false;
    }

    public boolean writeActors(List<Actor> actors) {
        return false;
    }
}
