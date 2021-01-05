import Data.ListPair;
import Data.PeerData;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import javafx.util.Pair;

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
        setPeers(peerss);
        this.clock = new VectorClock(peerss.size(),getVecPosition());

    }

    private void setPeers(List<Integer> peers) {
        this.peers = new ArrayList<>();
        this.peers.addAll(peers);
    }

    public void startServer() throws Exception {

        clientPutHandler();

        serverPutHandler();

        clientGetHandler();

        ms.start();

    }



    private int getVecPosition(){
        for(int i= 0;i <peers.size();i++){
            if(peers.get(i) == address){
                return i;
            }
        }
        return -1;
    }


    private void serverPutHandler(){
        ms.registerHandler("putServer", (a, m)-> { // m é do tipo ListPair
            System.out.println("Recebi um put de um server peer");
            // TODO: verificar se a msg verifica a regra causal



        }, es);

    }



    private void clientPutHandler(){

        ms.registerHandler("put", (a, m)-> {
            System.out.println("Recebido pedido de put de um cliente!");

            Map<Long,byte[]> values = (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(m);

            // Hashmap com os valores temporarios a mandar para os outros sv
            // em que a key é o servidor destino e como value tem a lista de pares a mandar
            Map<Integer, ListPair> tmp = new HashMap<>();

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

                int targetSv = peers.get(sv);

                if(targetSv == address){ // sou eu o dono da chave
                    keysValues.put(key,value);
                }else { // adicionar o par a lista do peer para dps mandar
                    if(tmp.containsKey(targetSv)){
                        tmp.get(targetSv).addPair(key,value);

                    }else {
                        ListPair lp = new ListPair();
                        lp.addPair(key,value);
                        tmp.put(targetSv,lp);
                    }

                }

            }

            if(!tmp.isEmpty()) sendKeysToRespectiveSv(tmp);

            System.out.println("Respondi ao pedido put do cliente");

        }, es);
    }


    // funçao aux
    private void sendKeysToRespectiveSv(Map<Integer, ListPair> data){
        // responder a todos os peers

        for(Map.Entry<Integer,ListPair> entry : data.entrySet()){
            int sv = entry.getKey();
            ListPair keys = entry.getValue();


            PeerData pdata = new PeerData(clock.incAndGetVectorClone(),keys);


            ms.sendAsync(Address.from("localhost",sv),"putServer",CollectionSerializer.getObjectInByte(pdata)).
                    thenRun(() -> {
                        System.out.println("Mensagem putServer enviada para peer");
                    })
                    .exceptionally(e -> {
                        e.printStackTrace();
                        return null;
                    });
        }

    }



    // TODO : fazer isto
    private void clientGetHandler(){
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
