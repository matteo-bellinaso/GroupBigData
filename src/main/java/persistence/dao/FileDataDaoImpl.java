package persistence.dao;

import persistence.ConnectionProvider;
import persistence.dao.interfaces.ActorDao;
import persistence.dao.interfaces.FileDataDao;
import persistence.dao.interfaces.RepoDao;
import persistence.entity.Actor;
import persistence.entity.FileData;
import persistence.entity.Repo;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class FileDataDaoImpl implements FileDataDao {

    @Override
    public List<FileData> getAllFileData(boolean withRel) {
        Connection conn = null;
        ResultSet result = null;
        Statement statement = null;
        List<FileData> fileDataList = null;
        try {
            conn = ConnectionProvider.openConnection();
            statement = conn.createStatement();
            result = statement.executeQuery("SELECT * FROM project_tables.file_data");

            fileDataList = fromRsToFd(result, withRel);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, statement, conn);
        }
        return fileDataList;
    }

    @Override
    public FileData getFileDataById(Long id, boolean withRel) {
        Connection conn = null;
        ResultSet result = null;
        List<FileData> fileDataList = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("SELECT * FROM project_tables.file_data where id_file= ?");
            preparedStatement.setLong(1, id);
            result = preparedStatement.executeQuery();
            fileDataList = fromRsToFd(result, withRel);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, preparedStatement, conn);
        }
        if (fileDataList != null && fileDataList.size() > 0) {
            return fileDataList.get(0);
        }

        return null;
    }

    @Override
    public List<FileData> getFileDatabyCreationDate(Date date, boolean withRel) {
        Connection conn = null;
        ResultSet result = null;
        List<FileData> fileDataList = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("SELECT * FROM project_tables.file_data where created_at= ?");
            preparedStatement.setDate(1, date);
            result = preparedStatement.executeQuery();
            fileDataList = fromRsToFd(result, withRel);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeResultSetAndStatementAndConnection(result, preparedStatement, conn);
        }

        return fileDataList;
    }

    @Override
    public boolean writeFileData(FileData fileData, boolean withRel) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = writeFileDataUtility(conn, fileData, withRel);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);

    }

    @Override
    public boolean updateFileData(FileData fileData, boolean withRel) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = conn.prepareStatement("UPDATE project_tables.file_data " +
                    "SET id_file=?, type=?, public=?, created_at=?,id_actor=?, id_repo=? WHERE id_file=?");

            preparedStatement.setObject(1, fileData.getId_file());
            preparedStatement.setObject(2, fileData.getType());
            preparedStatement.setObject(3, fileData.isPubblico());
            preparedStatement.setDate(4, fileData.getCreated_at());
            preparedStatement.setObject(5, fileData.getActor().getId());
            preparedStatement.setObject(6, fileData.getRepo().getId());
            preparedStatement.setObject(7, fileData.getId_file());
            result = preparedStatement.executeUpdate();
            if (withRel) {
                if (fileData.getActor() != null) {
                    new ActorDaoImpl().updateActor(fileData.getActor());
                }

                if (fileData.getRepo() != null) {
                    new RepoDaoImpl().updateRepo(fileData.getRepo());
                }
            }

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
    public boolean writeFileDatas(List<FileData> fileDatas, boolean withRel) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = ConnectionProvider.openConnection();
            for (FileData fileData : fileDatas) {
                preparedStatement = writeFileDataUtility(conn, fileData, withRel);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);


    }

    @Override
    public boolean deleteFileData(Long id, boolean withRel) {
        Connection conn = null;
        int result = 0;
        PreparedStatement preparedStatement = null;

        try {
            conn = ConnectionProvider.openConnection();
            preparedStatement = utilityDeleteFileData(conn, id, withRel);


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

    /*
     * @param listId list of id to delete
     * @return the number of the deleted row
     */
    @Override
    public int deleteFileDatas(List<Long> listId, boolean withRel) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        int result = 0;
        try {
            conn = ConnectionProvider.openConnection();
            for (Long id : listId) {
                preparedStatement = utilityDeleteFileData(conn, id, withRel);
                result = preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionProvider.closeStatementAndConnection(preparedStatement, conn);
        }

        return result;
    }


    private PreparedStatement writeFileDataUtility(Connection conn, FileData fileData, boolean withRel) throws SQLException {
        if (withRel) {
            if (fileData.getRepo() != null) {
                new RepoDaoImpl().writeRepo(fileData.getRepo());
            }

            if (fileData.getActor() != null) {
                new ActorDaoImpl().writeActor(fileData.getActor());
            }
        }
        PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO project_tables.file_data (id_file,type, public,created_at,id_actor,id_repo) VALUES (?,?,?,?,?,?)");

        preparedStatement.setObject(1, fileData.getId_file());
        preparedStatement.setObject(2, fileData.getType());
        preparedStatement.setObject(3, fileData.isPubblico());
        preparedStatement.setDate(4, fileData.getCreated_at());
        preparedStatement.setObject(5, fileData.getActor().getId());
        preparedStatement.setObject(6, fileData.getRepo().getId());
        preparedStatement.execute();
        return preparedStatement;
    }

    private PreparedStatement utilityDeleteFileData(Connection conn, Long id, boolean withRel) throws SQLException {
        PreparedStatement preparedStatement;
        RepoDao repoDao = new RepoDaoImpl();
        ActorDao actorDao = new ActorDaoImpl();
        FileData fileData = null;

        preparedStatement = conn.prepareStatement("DELETE FROM project_tables.file_data where id_file= ?");

        preparedStatement.setLong(1, id);

        if (withRel) {

            fileData = getFileDataById(id, false);

            if (fileData != null && fileData.getRepo() != null && fileData.getRepo().getId() != null) {
                repoDao.deleteRepo(fileData.getRepo().getId());
            }

            if (fileData != null && fileData.getActor() != null && fileData.getActor().getId() != null) {
                actorDao.deleteActor(fileData.getActor().getId());
            }

        }
        return preparedStatement;
    }

    private List<FileData> fromRsToFd(ResultSet result, boolean withRel) throws SQLException {
        List<FileData> fileDataList = new ArrayList<FileData>();
        FileData fileData;
        ActorDao actorDao = new ActorDaoImpl();
        RepoDao repoDao = new RepoDaoImpl();
        while (result.next()) {
            Actor actor = new Actor();
            Repo repo = new Repo();

            if (result.getLong("id_actor") == 0) {
                actor.setId(null);
            } else {
                if (!withRel) {
                    actor.setId(result.getLong("id_actor"));
                } else {
                    actor = actorDao.getActorById(result.getLong("id_actor"));
                }
            }

            if (result.getLong("id_repo") == 0) {
                repo.setId(null);
            } else {
                if (!withRel) {
                    repo.setId(result.getLong("id_repo"));
                } else {
                    repo = repoDao.getRepoById(result.getLong("id_repo"));
                }
            }


            fileData = new FileData(result.getLong("id_file"),
                    actor,
                    repo,
                    result.getString("type"),
                    result.getBoolean("public"),
                    result.getDate("created_at"));
            fileDataList.add(fileData);
        }
        return fileDataList;
    }
}
