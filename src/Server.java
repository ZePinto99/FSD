import Data.ListPair;
import Data.PeerData;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import javafx.scene.chart.ScatterChart;
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
    private List<PeerData> msgQueue;

    public Server(List<Integer> peerss, int address){
        this.ms = new NettyMessagingService("nome", Address.from(address), new MessagingConfig());
        this.address = address;
        this.es = Executors.newScheduledThreadPool(1);
        this.keysValues = new HashMap<>();
        setPeers(peerss);
        this.clock = new VectorClock(peerss.size(),getVecPosition(this.address));
        this.msgQueue = new ArrayList<>();

    }

    private void setPeers(List<Integer> peers) {
        this.peers = new ArrayList<>();
        this.peers.addAll(peers);
    }

    public void startServer() {

        clientPutHandler();

        serverPutHandler();

        clientGetHandler();

        ms.start();

    }



    private int getVecPosition(int add){
        for(int i= 0;i <peers.size();i++){
            if(peers.get(i) == add){
                return i;
            }
        }
        return -1;
    }


    private void serverPutHandler(){

        ms.registerHandler("putServer", (a, m)-> { // m é do tipo PeerData
            System.out.println("Eu "+ address +" Recebi um put de um server peer " + a.port());
            PeerData msg = (PeerData) CollectionSerializer.getObjectFromByte(m);

            assert msg != null;
            boolean respostaCausal = clock.regraCausal(msg.getVectorTag(),getVecPosition(a.port()));

            if(respostaCausal){
                clock.updateSenderPosition(msg.getVectorTag(),getVecPosition(a.port()));
                for(Pair<Long, byte[]> entry : msg.getList().getLista()){
                    keysValues.put(entry.getKey(),entry.getValue());
                }
                sendKeysToRespectiveSv(msg.getOtherData());

                checkMsgQueue();

            }else {
                msgQueue.add(msg);
                System.out.println("Mensagem nao respeita a regra causal, adicionada a msgQueue");
            }


        }, es);

    }



    private void clientPutHandler(){

        ms.registerHandler("put", (a, m)-> {
            System.out.println("Recebido pedido de put de um cliente!");

            Map<Long,byte[]> values = (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(m);

            // Hashmap com os valores temporarios a mandar para os outros sv
            // em que a key é o servidor destino e como value tem a lista de pares a mandar
            Map<Integer, ListPair> tmp = new HashMap<>();
            for(int i = 0; i< peers.size();i++){
                if(i == getVecPosition(address))continue;
                tmp.put(peers.get(i),new ListPair());
            }

            // itera todas as chaves enviadas pelo cliente
            assert values != null;
            // lock a hahsmap
            //lock ao relogio

            for(Map.Entry<Long,byte[]> entry : values.entrySet()) {
                Long key = entry.getKey();
                byte[] value = entry.getValue();

                // para teste apenas, eliminar dps
                String s = new String(value, StandardCharsets.UTF_8);
                System.out.println("Value que o cliente mandou:" + s);

                // vai ver a onde pertence a chave

                int sv = KeyHash.giveHashOfKey(key,this.peers.size());

                int targetSv = 0;
                try {
                    targetSv = peers.get(sv);
                }catch (Exception e){
                    e.printStackTrace();
                }


                if(targetSv == address){ // sou eu o dono da chave
                    keysValues.put(key,value);
                }else { // adicionar o par a lista do peer para dps mandar
                    tmp.get(targetSv).addPair(key,value);
                }

            }

            sendKeysToRespectiveSv(tmp);

        }, es);
    }


    // funçao aux
    private void sendKeysToRespectiveSv(Map<Integer, ListPair> data){
        // responder a todos os peers
        List<Integer> tagclock = clock.incAndGetVectorClone();

        try {


            for (int i = 0; i < peers.size(); i++) {
                if (i == getVecPosition(address)) continue;
                if(!data.containsKey(peers.get(i))) continue;
                if (data.get(peers.get(i)).getLista().isEmpty()) continue;
                System.out.println("AQUI");

                int sv = peers.get(i);

                PeerData pdata = new PeerData(tagclock, data.get(sv), address);

                Map<Integer, ListPair> othersData = new HashMap<>();
                for (int j = i + 1; j < peers.size(); j++) {
                    if (j == getVecPosition(address)) continue;
                    if (data.get(peers.get(j)).getLista().isEmpty()) continue;
                    othersData.put(peers.get(j), data.get(peers.get(j)));
                    System.out.println("eu " + address + "mandei tb data de outros peers " + j);

                }
                pdata.setOtherData(othersData);

                ms.sendAsync(Address.from("localhost", sv), "putServer", CollectionSerializer.getObjectInByte(pdata)).
                        thenRun(() -> { // TODO AQUI
                            System.out.println("Eu" + this.address + "Mensagem putServer enviada para peer" + sv);
                            checkMsgQueue();

                        })
                        .exceptionally(e -> {
                            e.printStackTrace();
                            return null;
                        });
                return;
            }
        }catch (Exception e){e.printStackTrace();}

    }



    // TODO : fazer isto
    private void clientGetHandler(){
        ms.registerHandler("get", (a, m)-> {
            System.out.println("Recebi um get");


            checkMsgQueue();
            // TODO AQUI
        }, es);

    }


    private void checkMsgQueue(){
        boolean respostaCausal;

        for(PeerData msg : msgQueue){
            respostaCausal = clock.regraCausal(msg.getVectorTag(),getVecPosition(msg.getSender()));
            if(respostaCausal){
                System.out.println("Mensagem perdida finalmente lida");
                for(Pair<Long, byte[]> entry : msg.getList().getLista()){
                    keysValues.put(entry.getKey(),entry.getValue());
                }
                msgQueue.remove(msg);
            }
        }

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
