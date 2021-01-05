package Data;

import javafx.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ListPair implements Serializable {
    private List<Pair<Long, byte[]>> lista ;


    public ListPair(){
        lista  = new ArrayList<>();
    }

    public List<Pair<Long, byte[]>> getLista() {
        return lista;
    }

    public void setLista(List<Pair<Long, byte[]>> lista) {
        this.lista = lista;
    }

    public void addPair(Long key, byte[] data){
        Pair<Long, byte[]> pair = new Pair<>(key, data);
        this.lista.add(pair);

    }
}
