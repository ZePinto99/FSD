import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

//Servidor com relógio vetorial
//Regra da entrega causal
public class Server implements Runnable {
    private NettyMessagingService ms;
    private int address;
    private ScheduledExecutorService es;
    private Map<Long,byte[]> keysValues;
    private VectorClock clock;
    private List<Integer> peers;

    public Server(List<Integer> peerss, int address){
        this.ms = new NettyMessagingService("nome", Address.from(address), new MessagingConfig());
        this.address = address;
        this.es = Executors.newScheduledThreadPool(1);
        this.keysValues = new HashMap<>();
        this.clock = new VectorClock(getVecPosition(address));
        setPeers(peerss);

    }

    private void setPeers(List<Integer> peers) {
        this.peers = new ArrayList<>();
        for(int x : peers) this.peers.add(x);
    }

    public void startServer() throws Exception {

        readPutMessage();

        readGetMessage();

        ms.start();

    }



    private int getVecPosition(int address){
        int vectorPosition = -1;
        switch (address){
            case 12345:
                vectorPosition = 0;
                break;
            case 12346:
                vectorPosition = 1;
                break;
            case 12347:
                vectorPosition = 2;
                break;
            default:
                System.out.println("Warning: Invalid address");
                break;
        }
        return vectorPosition;
    }


    // TODO: talvez usar locks nas chaves devido a poder ter 2 clientes a escrever a msm chave
    private void readPutMessage(){
        int finalVectorPosition1 = clock.getVectorPosition();
        ms.registerHandler("put", (a, m)-> {
            System.out.println("Recebido pedido de put de um cliente!");
            Map<Long,byte[]> values = (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(m);

            // itera todas as chaves enviadas pelo cliente
            assert values != null;
            for(Map.Entry<Long,byte[]> entry : values.entrySet()) {
                Long key = entry.getKey();
                byte[] value = entry.getValue();

                // para teste apenas, eliminar dps
                String s = new String(value, StandardCharsets.UTF_8);
                System.out.println("Value que o cliente mandou:" + s);

                // vai ver a onde pertence a chave
                int sv = KeyHash.giveHashOfKey(key,this.peers.size());
                System.out.println("peer" + sv);


                if(peers.get(sv) == address){ // sou eu o dono da chave
                    keysValues.put(key,value);
                }else { // TODO: mandar a {chave,valor} para o peer responsavel por ela

                }



            }


            //hash da mensagem
            //ver se é para mim ou não
            /*
            String messageRecive = new String(m);
            String[] key_value = messageRecive.split(",");
            //se for para mim guardo e atualizo o relógio
            if (Integer.parseInt(key_value[0]) == finalVectorPosition1){
                System.out.println("Guardei a mensagem");
            }
            else System.out.println("A mensagem não é para mim");
            */
            //
            System.out.println("Respondi ao pedido put");
        }, es);

        ms.registerHandler("putServer", (a, m)-> {
            System.out.println("Recebi um put do servidor"); // assumo que vem na msm em hashmap?

        }, es);
    }

    private void readGetMessage(){
        ms.registerHandler("get", (a, m)-> {
            System.out.println("Recebi um get");
        }, es);
    }

    @Override
    public void run() {
        try {
            startServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
