import Data.ListPair;
import Data.PeerData;

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

        long num2 = Long.MAX_VALUE-1;
        teste.put(num2,"atuaprima".getBytes());



        c.put(teste);
        /*
        List<Integer> tag = new ArrayList<>();
        tag.add(0,0);
        tag.add(1,2);
        tag.add(2,0);
        tag.add(3,0);


        ListPair lp = new ListPair();
        lp.addPair(30L,"fodadse".getBytes());
        PeerData pd = new PeerData(tag,lp,12346);
        c.teste(pd);
         */


    }

}
