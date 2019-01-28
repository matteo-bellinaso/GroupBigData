package persistence.dao.interfaces;


import persistence.entity.FileData;
import persistence.entity.Repo;

import java.sql.Date;
import java.util.List;

public interface FileDataDao {

    List<FileData> getAllFileData(boolean withRel);

    FileData getFileDataById(Long id,boolean withRel);

    List<FileData> getFileDatabyCreationDate(Date date, boolean withRel);

    boolean writeFileData(FileData fileData, boolean withRel);

    boolean updateFileData(FileData fileData, boolean withRel);

    boolean writeFileDatas(List<FileData> fileDatas);

    boolean deleteFileData(Long id);

    int deleteFileDatas(List<Long> listId);





}
