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
        this.nserver = NServers;
        this.vector = new ArrayList<>(Collections.nCopies(nserver, 0));
        this.vectorPosition = vectorPosition;

    }

    /*
     * Funçao que verifica a causualidade da mensagem enviada
     */
    public boolean regraCausal(List<Integer> messageVector, int sender){


        if (vector.get(sender) + 1 != (messageVector.get(sender))){
            return false;
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

        try {
            this.lock.lock();
            //System.out.println(vectorPosition +" - Dei lock ao clock");
        }catch (Exception e){e.printStackTrace();}



    }

    public void unLock(){
       try{
           lock.unlock();
           //System.out.println(vectorPosition + " - Dei unlock ao clock");
       }catch (Exception e){
           //System.out.println("Excessao no vector clock");
           e.printStackTrace();
       }



    }

    public boolean teste (){
        return this.lock.isLocked();
    }

}

