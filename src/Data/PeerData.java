package Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class PeerData implements Serializable {

    private List<Integer> vectorTag;

    private ListPair dados;


    public PeerData(List<Integer> tag,ListPair valores){
        this.dados = valores;
         this.vectorTag = tag;


    }


    public List<Integer> getVectorTag() {
        return vectorTag;
    }

    public void setVectorTag(List<Integer> vectorTag) {
        this.vectorTag = vectorTag;
    }

    public ListPair getTeste() {
        return dados;
    }

    public void setTeste(ListPair teste) {
        this.dados = teste;
    }
}
