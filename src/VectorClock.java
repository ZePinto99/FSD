import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VectorClock {
    private List<Integer> vector;
    private int vectorPosition;
    private int nserver;

    public VectorClock(int NServers,int vectorPosition){
        this.vector = new ArrayList<>(Collections.nCopies(NServers, 0));
        this.vectorPosition = vectorPosition;
        this.nserver = NServers;
    }

    public boolean regraCausal(List<Integer> messageVector, int sender){
        //ver se a mensagem é válida
        //l[i] + 1 = r[i]
        try {


            if (vector.get(sender) + 1 != (messageVector.get(sender)))
                return false;
            for (int i = 0; i < nserver; i++)
                if (i != sender && vector.get(i) < (messageVector.get(i))) {
                    return false;
                }
        }catch (Exception e){e.printStackTrace();}
        return true;
    }

    public void incrementPosition(){

        vector.set(vectorPosition, vector.get(vectorPosition) + 1);


    }

    // assume que a tag deu vdd na regraCausal
    public void updateSenderPosition(List<Integer> v, int sender){

        for(int i = 0; i < vector.size();i++){
            vector.set(i,Math.max(vector.get(i),v.get(i)));
        }
    }



    public List<Integer> getVector() {
        return vector;
    }

    public List<Integer> incAndGetVectorClone(){
        incrementPosition();
        List<Integer> arr = new ArrayList<>();
        arr.addAll(vector);
        return arr;

    }

    public void setVector(List<Integer> vector) {
        this.vector = vector;
    }

    public int getVectorPosition() {
        return vectorPosition;
    }

    public void setVectorPosition(int vectorPosition) {
        this.vectorPosition = vectorPosition;
    }
}
