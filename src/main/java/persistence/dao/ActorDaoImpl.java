package persistence.dao;

import persistence.ConnectionProvider;
import persistence.dao.interfaces.ActorDao;
import persistence.entity.Actor;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ActorDaoImpl implements ActorDao, Serializable {
    @Override
    public List<Actor> getAllActor() {
        Connection conn = null;
        ResultSet result = null;
        Statement statement = null;
        List<Actor> actorList = null;
        try {
            conn = ConnectionProvider.openConnection();
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT * FROM project_tables.actor");

            actorList = fromRsToActor(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, statement, conn);
        }

        return actorList;
    }

    @Override
    public Actor getActorById(Long id) {
        Connection conn = null;
        ResultSet result = null;
        List<Actor> actorList = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("SELECT * FROM project_tables.actor where id_actor= ?");
            preparedStatement.setLong(1, id);
            result = preparedStatement.executeQuery();
            actorList = fromRsToActor(result);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, preparedStatement, conn);
        }
        if (actorList != null && actorList.size() > 0) {
            return actorList.get(0);
        }

        return null;
    }

    @Override
    public boolean writeActor(Actor actor) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            if (getActorById(actor.getId()) == null) {
                conn = ConnectionProvider.openConnection();
                preparedStatement = writeActor(conn, actor);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);

    }

    @Override
    public boolean writeActors(List<Actor> actors) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();

            for (Actor actor : actors) {
                preparedStatement = writeActor(conn, actor);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);

    }

    @Override
    public boolean updateActor(Actor actor) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("UPDATE project_tables.actor " +
                    "SET id_actor=?, login=?, display_login=?, gravatar_id=?,url=?, avatar_url=? WHERE id_actor=?");

            preparedStatement.setObject(1, actor.getId());
            preparedStatement.setObject(2, actor.getLogin());
            preparedStatement.setObject(3, actor.getDisplay_login());
            preparedStatement.setObject(4, actor.getGravatar_id());
            preparedStatement.setObject(5, actor.getUrl());
            preparedStatement.setObject(6, actor.getAvatar_url());
            preparedStatement.setObject(7, actor.getId());
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
    public boolean deleteActor(Long id) {
        Connection conn = null;
        int result = 0;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = utilityDeleteActor(conn, id);
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
    public int deleteActors(List<Long> listId) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            for (Long id : listId) {
                preparedStatement = utilityDeleteActor(conn, id);
                result = preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
        }

        return result;
    }

    private PreparedStatement writeActor(Connection conn, Actor actor) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO project_tables.actor (id_actor,login, display_login,gravatar_id,url,avatar_url) VALUES (?,?,?,?,?,?)");

        preparedStatement.setObject(1, actor.getId());
        preparedStatement.setObject(2, actor.getLogin());
        preparedStatement.setObject(3, actor.getDisplay_login());
        preparedStatement.setObject(4, actor.getGravatar_id());
        preparedStatement.setObject(5, actor.getUrl());
        preparedStatement.setObject(6, actor.getAvatar_url());
        preparedStatement.execute();
        return preparedStatement;
    }

    private PreparedStatement utilityDeleteActor(Connection conn, Long id) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("DELETE FROM project_tables.actor where id_actor= ?");
        preparedStatement.setLong(1, id);

        return preparedStatement;
    }

    private List<Actor> fromRsToActor(ResultSet result) throws SQLException {
        List<Actor> actorList = new ArrayList<Actor>();
        Actor actor;
        while (result.next()) {

            actor = new Actor(result.getLong("id_actor"),
                    result.getString("login"),
                    result.getString("display_login"),
                    result.getString("gravatar_id"),
                    result.getString("url"),
                    result.getString("avatar_url"));
            actorList.add(actor);
        }
        return actorList;
    }
}
