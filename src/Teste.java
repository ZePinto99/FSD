import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Teste {
    public static void main(String[] args) throws Exception {
        List<Integer> nodes = new ArrayList<>();
        nodes.add(12345);
        ClientLibrary c = new ClientLibrary(nodes,8000);


        Map<Long,byte[]> teste = new HashMap<>();
        long num1=10L;


        teste.put(num1,"a".getBytes());

        long num2 = Long.MAX_VALUE-1;
        teste.put(num2,"a".getBytes());


        ClientLibrary c2 = new ClientLibrary(nodes,8000);


        Map<Long,byte[]> teste2 = new HashMap<>();


        teste2.put(num1,"b".getBytes());


        teste2.put(num2,"b".getBytes());


        c.put(teste);
        //c2.put(teste2);


        List<Long> lista = new ArrayList<>();
        lista.add(10L);


        /*c.get(lista).thenAccept((map) ->{
            if(map.keySet().isEmpty())System.out.println("RESPOSTA VAZIA");
            String s = new String(map.get(10L), StandardCharsets.UTF_8);
            System.out.println(" - Value que o cliente mandou:" + s);

        });*/

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
