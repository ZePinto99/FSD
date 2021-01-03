import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Teste {
    public static void main(String[] args) throws Exception {
        List<Integer> nodes = new ArrayList<>();
        nodes.add(12345);
        ClientLibrary c = new ClientLibrary(nodes);


        Map<Long,byte[]> teste = new HashMap<>();
        long num1=10L;


        teste.put(num1,"atuamae".getBytes());

        c.put(teste);




    }

}
