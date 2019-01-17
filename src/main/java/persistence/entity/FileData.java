package persistence.entity;

import java.sql.Date;

public class FileData {
    private Long id_file;
    private Long id_actor;
    private Long id_repo;
    private String type;
    private boolean pubblico;
    private Date created_at;

    public FileData(Long id_file, Long id_actor, Long id_repo, String type, boolean pubblico, Date created_at) {
        this.id_file = id_file;
        this.id_actor = id_actor;
        this.id_repo = id_repo;
        this.type = type;
        this.pubblico = pubblico;
        this.created_at = created_at;
    }

    public Long getId_file() {
        return id_file;
    }

    public void setId_file(Long id_file) {
        this.id_file = id_file;
    }

    public Long getId_actor() {
        return id_actor;
    }

    public void setId_actor(Long id_actor) {
        this.id_actor = id_actor;
    }

    public Long getId_repo() {
        return id_repo;
    }

    public void setId_repo(Long id_repo) {
        this.id_repo = id_repo;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isPubblico() {
        return pubblico;
    }

    public void setPubblico(boolean pubblico) {
        this.pubblico = pubblico;
    }

    public Date getCreated_at() {
        return created_at;
    }

    public void setCreated_at(Date created_at) {
        this.created_at = created_at;
    }
}
