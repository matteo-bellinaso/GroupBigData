package persistence.entity;


import java.util.Objects;

public class Actor {
    private Long id;
    private String login;
    private String display_login;
    private String gravatar_id;
    private String url;
    private String avatar_url;

    public Actor() {
        id = null;
        login = null;
        display_login = null;
        gravatar_id = null;
        url = null;
        avatar_url = null;
    }

    public Actor(Long id, String login, String display_login, String gravatar_id, String url, String avatar_url) {
        this.id = id;
        this.login = login;
        this.display_login = display_login;
        this.gravatar_id = gravatar_id;
        this.url = url;
        this.avatar_url = avatar_url;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getDisplay_login() {
        return display_login;
    }

    public void setDisplay_login(String display_login) {
        this.display_login = display_login;
    }

    public String getGravatar_id() {
        return gravatar_id;
    }

    public void setGravatar_id(String gravatar_id) {
        this.gravatar_id = gravatar_id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAvatar_url() {
        return avatar_url;
    }

    public void setAvatar_url(String avatar_url) {
        this.avatar_url = avatar_url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Actor)) return false;

        Actor actor = (Actor) o;

        if (getId() != null ? !getId().equals(actor.getId()) : actor.getId() != null) return false;
        if (getLogin() != null ? !getLogin().equals(actor.getLogin()) : actor.getLogin() != null) return false;
        if (getDisplay_login() != null ? !getDisplay_login().equals(actor.getDisplay_login()) : actor.getDisplay_login() != null)
            return false;
        if (getGravatar_id() != null ? !getGravatar_id().equals(actor.getGravatar_id()) : actor.getGravatar_id() != null)
            return false;
        if (getUrl() != null ? !getUrl().equals(actor.getUrl()) : actor.getUrl() != null) return false;
        return getAvatar_url() != null ? getAvatar_url().equals(actor.getAvatar_url()) : actor.getAvatar_url() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getLogin() != null ? getLogin().hashCode() : 0);
        result = 31 * result + (getDisplay_login() != null ? getDisplay_login().hashCode() : 0);
        result = 31 * result + (getGravatar_id() != null ? getGravatar_id().hashCode() : 0);
        result = 31 * result + (getUrl() != null ? getUrl().hashCode() : 0);
        result = 31 * result + (getAvatar_url() != null ? getAvatar_url().hashCode() : 0);
        return result;
    }
}
