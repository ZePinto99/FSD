package Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PeerDataPut implements Serializable{

    private List<Integer> vectorTag;

    private ListPair yourdados;

    private Map<Integer, ListPair> otherData;


    private int sender;

    public PeerDataPut(List<Integer> tag, ListPair valores, int sender){
        this.sender = sender;
        this.yourdados = valores;
        this.vectorTag = tag;
        this.otherData = new HashMap<>();


    }


    public Map<Integer, ListPair> getOtherData() {
        return otherData;
    }

    public void setOtherData(Map<Integer, ListPair> otherData) {
        this.otherData = otherData;
    }

    public int getSender() {
        return sender;
    }

    public void setSender(int sender) {
        this.sender = sender;
    }

    public List<Integer> getVectorTag() {
        return vectorTag;
    }

    public void setVectorTag(List<Integer> vectorTag) {
        this.vectorTag = vectorTag;
    }

    public ListPair getList() {
        return yourdados;
    }

    public void setList(ListPair teste) {
        this.yourdados = teste;
    }
}
