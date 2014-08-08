package collene;

import java.io.IOException;

public interface Translate {
    public String translate(String key) throws IOException;
    public void setTranslation(String key, String translation) throws IOException;
    public void unset(String key) throws IOException;
}
