import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VectorClock {
    private List<Integer> vector;
    private int vectorPosition;

    public VectorClock(int vectorPosition){
        this.vector = new ArrayList<>(Collections.nCopies(3, 0));
        this.vectorPosition = vectorPosition;
    }

    public boolean regraCausal(String[] messageVector){
        //ver se a mensagem é válida
        boolean causalBool = true;
        //l[i] + 1 = r[i]
        if (vector.get(vectorPosition) + 1 != Integer.parseInt(messageVector[vectorPosition]))
            causalBool = false;
        for (int i = 0; i < 3; i++)
            if (i != vectorPosition && vector.get(i) < Integer.parseInt(messageVector[i])) {
                causalBool = false;
                break;
            }
        return causalBool;
    }

    public void incrementPosition(){
        vector.add(vectorPosition, vector.get(vectorPosition) + 1);
    }

    public List<Integer> getVector() {
        return vector;
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
