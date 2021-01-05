import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VectorClock {
    private List<Integer> vector;
    private int vectorPosition;

    public VectorClock(int NServers,int vectorPosition){
        this.vector = new ArrayList<>(Collections.nCopies(NServers, 0));
        this.vectorPosition = vectorPosition;
    }

    public boolean regraCausal(String[] messageVector, int sender){
        //ver se a mensagem é válida
        boolean causalBool = true;
        //l[i] + 1 = r[i]
        if (vector.get(sender) + 1 != Integer.parseInt(messageVector[sender]))
            causalBool = false;
        for (int i = 0; i < 3; i++)
            if (i != sender && vector.get(i) < Integer.parseInt(messageVector[i])) {
                causalBool = false;
                System.out.println(causalBool);
                break;
            }
        return causalBool;
    }

    public void incrementPosition(){
        try {
            vector.set(vectorPosition, vector.get(vectorPosition) + 1);
        }catch (Exception e){
            e.printStackTrace();
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
