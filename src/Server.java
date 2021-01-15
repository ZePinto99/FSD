import Data.*;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import javafx.util.Pair;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
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
    private List<PeerDataPut> msgQueue;

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

    private void serverPutHandler(){

        ms.registerHandler("putServer", (a, m)-> { // m é do tipo PeerData
            System.out.println(address +" - Recebi um put de um server peer " + a.port());
            PeerDataPut msg = (PeerDataPut) CollectionSerializer.getObjectFromByte(m);

            this.clock.lock();

            System.out.println(address +" - MY VECTOR "+ this.clock.getVector());
            assert msg != null;
            System.out.println(address +" - Put - VECTOR RECEBIDO "+ msg.getVectorTag());
            boolean respostaCausal = clock.regraCausal(msg.getVectorTag(),getVecPosition(a.port()));
            System.out.println(address + " - Put - RESPOSTA DA REGRA CAUSAL: "+ respostaCausal);


            if(respostaCausal){
                clock.updateVectorClock(msg.getVectorTag());

                lockKeys(msg.getList().getLista());

                writeKeysInHashMap(msg.getList().getLista());

                sendKeysToRespectiveSv(msg.getOtherData());

                try {

                    this.clock.unLock();

                    unlockKeys(msg.getList().getLista());
                }catch (Exception e){
                    e.printStackTrace();
                }


                System.out.println(address + " - Respondi ao putServer do " + a.port());
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

    private void clientGetHandler(){
        ms.registerHandler("get", (a, m)-> {
            System.out.println(address + " - Recebi um get");
            Collection<Long> dados = (Collection<Long>) CollectionSerializer.getObjectFromByte(m);

            assert dados != null;
            ArrayList<Pair<Long, byte[]>> tmpmine = new ArrayList<>();

            Map<Integer, List<Long>> tmpOthers = new HashMap<>();
            for (long chave : dados) {
                int sv = KeyHash.giveHashOfKey(chave, peers.size());

                int targetSv = peers.get(sv);
                if (targetSv == address) {
                    byte[] emptyArray = new byte[0];
                    Pair<Long, byte[]> p = new Pair<>(chave,emptyArray);
                    tmpmine.add(p);
                } else {
                    if (!tmpOthers.containsKey(targetSv)) tmpOthers.put(targetSv, new ArrayList<>());
                    tmpOthers.get(targetSv).add(chave);
                }

            }
            Map<Long,byte[]> response = new HashMap<>();

            lockKeys(tmpmine);
            this.clock.lock();

            getKeysOfHashMap(tmpmine,response);

            try {
                sendGetRequestToRespectivSv(tmpOthers,response);
                unlockKeys(tmpmine);
            }catch (Exception e){
                e.printStackTrace();
            }

            System.out.println(address + " Respondi ao get do cliente" + a.port());
            return CollectionSerializer.getObjectInByte(response);
        }, es);

    }

    private void ServerGetHandler(){
        ms.registerHandler("getServer", (a, m)-> {
            System.out.println(address +" - Recebi um get de um Server");
            PeerDataGet dados = (PeerDataGet) CollectionSerializer.getObjectFromByte(m);
            Map<Long, byte[]> resp = new HashMap<>();
            this.clock.lock();
            System.out.println(address +" - Get - VECTOR RECEBIDO "+ dados.getVectorTag());
            boolean respostaCausal = clock.regraCausal(dados.getVectorTag(),getVecPosition(a.port()));
            System.out.println(address + " - Get - RESPOSTA DA REGRA CAUSAL: "+ respostaCausal);

            if(respostaCausal){
                clock.updateVectorClock(dados.getVectorTag());
                this.hashMapLock.lock();
                for(long key : dados.getListKeys()){
                    if(keysValues.containsKey(key)) {
                        resp.put(key, keysValues.get(key).getDados());
                    }else {
                        resp.put(key,null);
                    }
                }
                this.hashMapLock.unlock();
            }
            this.clock.unLock();

            System.out.println(address + " - Respondi a um GetServer");
            return CollectionSerializer.getObjectInByte(resp);


        }, es);

    }

    private void ClockUpdateHandler(){
        ms.registerHandler("updateClock", (a, m)-> { //m  é lista de inteiros
            List<Integer> tag = (List<Integer>) CollectionSerializer.getObjectFromByte(m);
            this.clock.lock();
            this.clock.updateVectorClock(tag);
            System.out.println(address +" - Recebi um updateClock - Vetor atualiazdo " + this.clock.getVector());
            this.clock.unLock();

            checkMsgQueue();
        }, es);

    }


    private void writeKeysInHashMap(List< Pair<Long, byte[]>> lista){
        if(lista.size()==0)return;
        for(Pair<Long, byte[]> entry : lista){
            keysValues.put(entry.getKey(),new KeyValue(entry.getValue()));
        }
        System.out.println(address +" - ESCREVI CHAVES NA HASHMAP");

    }


    private void lockKeys(List<Pair<Long, byte[]>> lista){
        if(lista.size()==0)return;
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
        if(lista.size()==0)return;
        System.out.println(address +" - DEI UNLOCK AS KEYS");
        this.hashMapLock.lock();
        for(Pair<Long, byte[]> entry: lista){
            keysValues.get(entry.getKey()).unLock();
        }
        this.hashMapLock.unlock();
    }



    // funçao aux
    private void sendKeysToRespectiveSv(Map<Integer, ListPair> data){
        try {

        if(data.keySet().isEmpty()) return;

        int minSv = Integer.MAX_VALUE;

        for(int x : data.keySet()){
             if(x < minSv) minSv = x;
        }

        if(minSv == Integer.MAX_VALUE) return;

        List<Integer> tagclock = clock.incAndGetVectorClone();
        System.out.println(address +" - Put - tag do clock que envio " + tagclock);

        Map<Integer, ListPair> otherData = new HashMap<>();

        for(Map.Entry<Integer,ListPair> entry : data.entrySet()){
            if(entry.getKey()== minSv) continue;
            otherData.put(entry.getKey(),entry.getValue());
        }

        PeerDataPut pdata = new PeerDataPut(tagclock,data.get(minSv),address);
        pdata.setOtherData(otherData);


        int finalMinSv = minSv;
        ms.sendAsync(Address.from("localhost", minSv), "putServer", CollectionSerializer.getObjectInByte(pdata)).
                thenRun(() -> {
                    System.out.println(this.address + " - Mensagem putServer enviada para peer "+ finalMinSv + " com o clock " + tagclock);
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
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("AQUIIIIIIII");
    }





    private void getKeysOfHashMap(ArrayList<Pair<Long, byte[]>> lista,Map<Long,byte[]> response){
        for(Pair<Long, byte[]> entry : lista){
            response.put(entry.getKey(),keysValues.get(entry.getKey()).getDados());
        }

    }

    //
    private void sendGetRequestToRespectivSv(Map<Integer, List<Long>> map,Map<Long,byte[]> response) throws Exception{
        if(map.keySet().isEmpty()) {
            this.clock.unLock();
            return;

        }

        List<Integer> tagclock = clock.incAndGetVectorClone();

        System.out.println(address +" - Get - tag do clock que envio " + tagclock);

        this.clock.unLock();

        CompletableFuture<byte[]> completableFuture;
        List<Integer> listaTagClock = new ArrayList<>();

        for(Map.Entry<Integer, List<Long>> entry : map.entrySet()) {
            PeerDataGet dados = new PeerDataGet(tagclock,entry.getValue());
            listaTagClock.add(entry.getKey());
            System.out.println(address + " - Enviei pedido de get ao sv " + entry.getKey());
            completableFuture = ms.sendAndReceive(Address.from("localhost", entry.getKey()), "getServer", CollectionSerializer.getObjectInByte(dados));

            byte[] bytes = completableFuture.get();

            Map<Long, byte[]> resposta = (Map<Long, byte[]>) CollectionSerializer.getObjectFromByte(bytes);

            if(resposta.keySet().isEmpty())System.out.println(address + "- A resposta do sv veio empty");

            for (Map.Entry<Long, byte[]> resp : resposta.entrySet()) {
                response.put(resp.getKey(), resp.getValue());
            }

        }




    }




    private void checkMsgQueue(){
        boolean respostaCausal;

        this.queueLock.lock();
        for(PeerDataPut mensagem : msgQueue){

            PeerDataPut msg = (PeerDataPut) mensagem;
            respostaCausal = clock.regraCausal(msg.getVectorTag(), getVecPosition(msg.getSender()));
            if (respostaCausal) {
                clock.updateVectorClock(msg.getVectorTag());
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

    private int getVecPosition(int add){
        for(int i= 0;i <peers.size();i++){
            if(peers.get(i) == add){
                return i;
            }
        }
        return -1;
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
