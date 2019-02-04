package persistence.entity;

import java.sql.Date;
import java.util.Objects;

public class FileData {
    private Long id_file;//pk
    private Actor actor; //actor fk
    private Repo repo; //repo fk
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileData)) return false;

        FileData fileData = (FileData) o;

        if (isPubblico() != fileData.isPubblico()) return false;
        if (!getId_file().equals(fileData.getId_file())) return false;
        if (getActor() != null ? !getActor().equals(fileData.getActor()) : fileData.getActor() != null) return false;
        if (getRepo() != null ? !getRepo().equals(fileData.getRepo()) : fileData.getRepo() != null) return false;
        if (getType() != null ? !getType().equals(fileData.getType()) : fileData.getType() != null) return false;
        return getCreated_at() != null ? getCreated_at().equals(fileData.getCreated_at()) : fileData.getCreated_at() == null;
    }

    @Override
    public int hashCode() {
        int result = getId_file().hashCode();
        result = 31 * result + (getActor() != null ? getActor().hashCode() : 0);
        result = 31 * result + (getRepo() != null ? getRepo().hashCode() : 0);
        result = 31 * result + (getType() != null ? getType().hashCode() : 0);
        result = 31 * result + (isPubblico() ? 1 : 0);
        result = 31 * result + (getCreated_at() != null ? getCreated_at().hashCode() : 0);
        return result;
    }
}
