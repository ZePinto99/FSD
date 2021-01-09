import Data.KeyValue;
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
import java.util.concurrent.locks.ReentrantLock;

//Servidor com relógio vetorial
//Regra da entrega causal
public class Server implements Runnable {
    private ReentrantLock hashMapLock;
    private ReentrantLock queueLock;

    private NettyMessagingService ms;
    private ScheduledExecutorService es;
    private int address;
    private List<Integer> peers;

    private Map<Long,KeyValue> keysValues;
    private VectorClock clock;
    private List<PeerData> msgQueue;

    public Server(List<Integer> peerss, int address){
        this.ms = new NettyMessagingService("nome", Address.from(address), new MessagingConfig());
        this.address = address;
        this.es = Executors.newScheduledThreadPool(4);
        this.keysValues = new HashMap<>();
        this.hashMapLock = new ReentrantLock();
        this.queueLock = new ReentrantLock();
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

        ServerGetHandler();

        ClockUpdateHandler();

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
            System.out.println(address +" -  Recebi um put de um server peer " + a.port());
            PeerData msg = (PeerData) CollectionSerializer.getObjectFromByte(m);

            this.clock.lock();

            System.out.println(address +" - MY VECTOR "+ this.clock.getVector());
            assert msg != null;
            System.out.println(address +" - VECTOR RECEBIDO "+ msg.getVectorTag());
            boolean respostaCausal = clock.regraCausal(msg.getVectorTag(),getVecPosition(a.port()));
            System.out.println(address + " - RESPOSTA DA REGRA CAUSAL: "+ respostaCausal);


            if(respostaCausal){
                clock.updateVectorClock(msg.getVectorTag());

                lockKeys(msg.getList().getLista());

                writeKeysInHashMap(msg.getList().getLista());

                sendKeysToRespectiveSv(msg.getOtherData());

                this.clock.unLock();

                unlockKeys(msg.getList().getLista());

                checkMsgQueue();
            }else {
                this.clock.unLock();
                this.queueLock.lock();
                msgQueue.add(msg);
                this.queueLock.unlock();
                System.out.println(address +" - Mensagem nao respeita a regra causal, adicionada a msgQueue");
            }


        }, es);

    }

    private void writeKeysInHashMap(List< Pair<Long, byte[]>> lista){
        for(Pair<Long, byte[]> entry : lista){
            keysValues.put(entry.getKey(),new KeyValue(entry.getValue()));
        }
        System.out.println(address +" - ESCREVI CHAVES NA HASHMAP");

    }


    private void lockKeys(List<Pair<Long, byte[]>> lista){

        this.hashMapLock.lock();
        for(Pair<Long, byte[]> entry: lista){
            if(!keysValues.containsKey(entry.getKey())) {
                keysValues.put(entry.getKey(), new KeyValue());
            }
            keysValues.get(entry.getKey()).lock();

        }

        this.hashMapLock.unlock();


        System.out.println(address +" - DEI LOCK AS KEYS");
    }

    private void unlockKeys(List<Pair<Long, byte[]>> lista){
        System.out.println(address +" - DEI UNLOCK AS KEYS");
        this.hashMapLock.lock();
        for(Pair<Long, byte[]> entry: lista){
            keysValues.get(entry.getKey()).unLock();
        }
        this.hashMapLock.unlock();
    }


    private void clientPutHandler(){

        ms.registerHandler("put", (a, m)-> {
            System.out.println(address +" - Recebido pedido de put de um cliente!");

            Map<Long,byte[]> values = (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(m);

            // Hashmap com os valores temporarios a mandar para os outros sv
            // em que a key é o servidor destino e como value tem a lista de pares a mandar
            Map<Integer, ListPair> tmp = new HashMap<>();

            // lista temporaria com os valores que tenho de por na hashmap local
            List<Pair<Long, byte[]>> tmpList = new ArrayList<>();

            // itera todas as chaves enviadas pelo cliente
            assert values != null;
            // lock a hahsmap
            //lock ao relogio

            for(Map.Entry<Long,byte[]> entry : values.entrySet()) {
                Long key = entry.getKey();
                byte[] value = entry.getValue();

                // para teste apenas, eliminar dps
                String s = new String(value, StandardCharsets.UTF_8);
                System.out.println(address +" - Value que o cliente mandou:" + s);

                // vai ver a onde pertence a chave




                int sv = KeyHash.giveHashOfKey(key,this.peers.size());

                int targetSv = 0;

                targetSv = peers.get(sv);

                if(targetSv == address){ // sou eu o dono da chave
                    Pair<Long, byte[]> p = new Pair<>(key,value);
                    tmpList.add(p);
                }else { // adicionar o par a lista do peer para dps mandar
                    if(!tmp.containsKey(targetSv))tmp.put(targetSv,new ListPair());
                    tmp.get(targetSv).addPair(key,value);
                }


            }

            lockKeys(tmpList);
            this.clock.lock();

            writeKeysInHashMap(tmpList);
            sendKeysToRespectiveSv(tmp);

            this.clock.unLock();
            unlockKeys(tmpList);

        }, es);
    }


    // funçao aux
    private void sendKeysToRespectiveSv(Map<Integer, ListPair> data){

        if(data.keySet().isEmpty()) return;

        int minSv = Integer.MAX_VALUE;

        for(int x : data.keySet()){
             if(x < minSv) minSv = x;
        }

        if(minSv == Integer.MAX_VALUE) return;

        List<Integer> tagclock = clock.incAndGetVectorClone();
        System.out.println(address +" - tag do clock que envio " + tagclock);

        Map<Integer, ListPair> otherData = new HashMap<>();

        for(Map.Entry<Integer,ListPair> entry : data.entrySet()){
            if(entry.getKey()== minSv) continue;
            otherData.put(entry.getKey(),entry.getValue());
        }

        PeerData pdata = new PeerData(tagclock,data.get(minSv),address);
        pdata.setOtherData(otherData);


        ms.sendAsync(Address.from("localhost", minSv), "putServer", CollectionSerializer.getObjectInByte(pdata)).
                thenRun(() -> {
                    //System.out.println("Eu " + this.address + "Mensagem putServer enviada para peer "+ finalMinSv);
                    checkMsgQueue();

                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                });

        for(int i = 0; i< peers.size();i++){
            if(peers.get(i)== minSv || peers.get(i) == address) continue;
            ms.sendAsync(Address.from("localhost", peers.get(i)), "updateClock", CollectionSerializer.getObjectInByte(tagclock)).
                    exceptionally(e -> {
                        e.printStackTrace();
                        return null;
                    });


        }

}



    private void ClockUpdateHandler(){
        ms.registerHandler("updateClock", (a, m)-> { //m  é lista de inteiros
            System.out.println(address +" - Recebi um updateClock");
            List<Integer> tag = (List<Integer>) CollectionSerializer.getObjectFromByte(m);
            this.clock.lock();
            this.clock.updateVectorClock(tag);
            System.out.println(address +" - Vetor atualiazdo " + this.clock.getVector());
            this.clock.unLock();

            checkMsgQueue();
        }, es);

    }



    // TODO : fazer isto
    private void clientGetHandler(){
        ms.registerHandler("get", (a, m)-> {
            System.out.println("Recebi um get");

            
        }, es);

    }


    private void ServerGetHandler(){


    }


    private void checkMsgQueue(){
        boolean respostaCausal;

        this.queueLock.lock();
        for(PeerData msg : msgQueue){
            respostaCausal = clock.regraCausal(msg.getVectorTag(),getVecPosition(msg.getSender()));
            if(respostaCausal){
                lockKeys(msg.getList().getLista());
                this.clock.lock();
                System.out.println("Mensagem perdida finalmente lida");

                writeKeysInHashMap(msg.getList().getLista());
                sendKeysToRespectiveSv(msg.getOtherData());

                unlockKeys(msg.getList().getLista());
                this.clock.unLock();

                msgQueue.remove(msg);
            }
        }
        this.queueLock.unlock();

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
