package persistence.dao;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import persistence.DatabasePreparator;
import persistence.dao.interfaces.ActorDao;
import persistence.entity.Actor;


import java.util.List;

import static org.junit.Assert.*;

public class ActorDaoImplTest extends DatabasePreparator {



    static ActorDao dao;

    @BeforeClass
    public static void initialize() {

        dao = new ActorDaoImpl();
    }

    @Test
    public void writeActorTest() {
        assertTrue(writeOnDb(sampleGenerator()));
    }

    @Test
    public void readActor() {
        writeOnDb(sampleGenerator());
        List<Actor> fileData = dao.getAllActor();
        assertTrue(fileData.size() > 0);
    }

    @Test
    public void updateActor() {
        Actor actor = sampleGenerator();
        writeOnDb(actor);
        Actor result = dao.getActorById(actor.getId(), true);
        assertEquals(actor, result);
        actor.setAvatar_url("url modificato");
        dao.updateActor(actor);
        result = dao.getActorById(actor.getId(), true);
        assertEquals(actor, result);
    }

    private Actor sampleGenerator() {
        Long id = ((long) (Math.random() * 1000000));
        ids.add(id);
        return new Actor(id,"login","display_login","gravatar_id","url","avatar_url");

    }

    private boolean writeOnDb(Actor actor) {
        return dao.writeActor(actor);
    }

    @AfterClass
    public static void delete() {
        dao.deleteActors(ids);
    }

}