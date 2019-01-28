package persistence.entity;

import java.sql.Date;

public class FileData {
    private Long id_file;
    private Actor actor;
    private Repo repo;
    private String type;
    private boolean pubblico;
    private Date created_at;

    public FileData(Long id_file, Actor actor, Repo repo, String type, boolean pubblico, Date created_at) {
        this.id_file = id_file;
        this.actor = actor;
        this.repo = repo;
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

    public Actor getActor() {
        return actor;
    }

    public void setActor(Actor actor) {
        this.actor = actor;
    }

    public Repo getRepo() {
        return repo;
    }

    public void setRepo(Repo repo) {
        this.repo = repo;
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
