package persistence.entity;

import java.util.Objects;

public class Repo {
    private Long id;
    private String name;
    private String url;

    public Repo() {
        id = null;
        name = null;
        url = null;
    }


    public Repo(Long id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Repo)) return false;

        Repo repo = (Repo) o;

        if (getId() != null ? !getId().equals(repo.getId()) : repo.getId() != null) return false;
        if (getName() != null ? !getName().equals(repo.getName()) : repo.getName() != null) return false;
        return getUrl() != null ? getUrl().equals(repo.getUrl()) : repo.getUrl() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getUrl() != null ? getUrl().hashCode() : 0);
        return result;
    }
}
