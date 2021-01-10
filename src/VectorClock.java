import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class VectorClock {
    private ReentrantLock lock;
    private List<Integer> vector;
    private int vectorPosition;
    private int nserver;

    public VectorClock(int NServers,int vectorPosition){
        this.lock = new ReentrantLock();
        this.vector = new ArrayList<>(Collections.nCopies(NServers, 0));
        this.vectorPosition = vectorPosition;
        this.nserver = NServers;
    }

    public boolean regraCausal(List<Integer> messageVector, int sender){


        if (vector.get(sender) + 1 != (messageVector.get(sender))){
            return false;
        }
        for (int i = 0; i < nserver; i++) {
            if (i != sender && vector.get(i) > (messageVector.get(i))) {
                return false;
            }
        }
        return true;
    }

    public void incrementPosition(){
        vector.set(vectorPosition, vector.get(vectorPosition) + 1);


    }

    // assume que a tag deu vdd na regraCausal
    public void updateVectorClock(List<Integer> v){

        for(int i = 0; i < vector.size();i++){
            if(i == vectorPosition)continue;
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

    public void lock(){
        this.lock.lock();

    }

    public void unLock(){
        this.lock.unlock();
    }

}

