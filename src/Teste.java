import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

public class Teste {
    public static void main(String[] args) throws Exception {
        List<Integer> nodes = new ArrayList<>();
        nodes.add(12345);


        long num1=10L;
        long num2 = Long.MAX_VALUE-1;

        Map<Long,byte[]> mapcliente1 = new HashMap<>();
        mapcliente1.put(num1,"a".getBytes());
        mapcliente1.put(num2,"a".getBytes());


        Map<Long,byte[]> mapcliente2 = new HashMap<>();
        mapcliente2.put(num1,"b".getBytes());
        mapcliente2.put(num2,"b".getBytes());

        ClientLibrary client1 = new ClientLibrary(nodes,8000);

        ClientLibrary client2 = new ClientLibrary(nodes,8000);

        ClientLibrary client3 = new ClientLibrary(nodes,9000);

        client1.put(mapcliente1);
        client2.put(mapcliente2);


        List<Long> listaGet = new ArrayList<>();
        listaGet.add(10L);
        listaGet.add(Long.MAX_VALUE-1);

        /*
        * serve apenas para garantir que o pedido de Get é processado depois dos dois puts
        */
        sleep(100);

        client3.get(listaGet).thenAccept((map) ->{
            System.out.println("tamanho da lista recebida no get: " + map.keySet().size());
            for(Map.Entry<Long,byte[]> entry : map.entrySet()){
                if(entry.getValue() == null) {
                    System.out.println("Get com valor a nulo => get foi processado primeiro que o put");
                    continue;
                }
                String s = new String(entry.getValue(), StandardCharsets.UTF_8);
                System.out.println("Valor que a chave tem: " + s);
            }


        });



    }





}
