package persistence.dao.interfaces;


import persistence.entity.FileData;
import persistence.entity.Repo;

import java.sql.Date;
import java.util.List;

public interface FileDataDao {

    List<FileData> getAllFileData();

    FileData getFileDataById(Long id);

    List<FileData> getFileDatabyCreationData(Date date);

    boolean writeFileData(FileData fileData);

    boolean writeFileDatas(List<FileData> fileDatas);


}
