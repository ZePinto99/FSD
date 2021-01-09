package Data;

import java.io.Serializable;
import java.util.List;

public class PeerDataGet implements Serializable{

    private List<Integer> vectorTag;
    private List<Long> listKeys;

    public PeerDataGet(List<Integer> tag, List<Long> keys){
        this.vectorTag = tag;
        this.listKeys = keys;
    }

    public List<Integer> getVectorTag() {
        return vectorTag;
    }

    public void setVectorTag(List<Integer> vectorTag) {
        this.vectorTag = vectorTag;
    }

    public List<Long> getListKeys() {
        return listKeys;
    }

    public void setListKeys(List<Long> listKeys) {
        this.listKeys = listKeys;
    }

}
