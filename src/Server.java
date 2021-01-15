import Data.*;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import javafx.util.Pair;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

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

    /*
    * inicializa o servidor ,i.e, inicializa todos os handlers necessarios ao funcionamento do mesmo e dá start ao serviço
    */
    public void startServer() {

        clientPutHandler();

        serverPutHandler();

        clientGetHandler();

        ServerGetHandler();

        ClockUpdateHandler();

        ms.start();

    }


    /*
    * Handler que trata de pedidos de clientes nomeadamente de pedidos para por um conjunto de chaves no servidor
    */
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

            handleMessage(tmpList,tmp);

            this.clock.unLock();
            unlockKeys(tmpList);

        }, es);
    }

    private void handleMessage(List<Pair<Long, byte[]>> tmpList,Map<Integer, ListPair> tmp){
        int minSv = Integer.MAX_VALUE;

        for(int x : tmp.keySet()){
            if(x < minSv) minSv = x;
        }

        if(tmpList.size() != 0 && !tmp.isEmpty()){
            if(this.address > minSv){
                ListPair lista = new ListPair();
                lista.setLista(tmpList);
                tmp.put(address,lista);
                sendKeysToRespectiveSv(tmp);
            }else {
                writeKeysInHashMap(tmpList);
                sendKeysToRespectiveSv(tmp);
            }
        }else {
            writeKeysInHashMap(tmpList);
            sendKeysToRespectiveSv(tmp);
        }
    }




    /*
     * Handler que trata de pedidos de servidores em que é assumido que este é o responsavel pela chaves enviadas
     */
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


                unlockKeys(msg.getList().getLista());



                System.out.println(address + " - Respondi ao putServer do " + a.port());
                checkMsgQueue();
            }else {
                this.queueLock.lock();
                msgQueue.add(msg);
                this.queueLock.unlock();
                System.out.println(address +" - Mensagem nao respeita a regra causal, adicionada a msgQueue");
            }
            this.clock.unLock();


        }, es);

    }

    /*
     * Handler que trata de pedidos de clientes nomeadamente de pedidos para obter o valor de um dado conjunto de chaves
     */
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
                this.clock.unLock();
                unlockKeys(tmpmine);
            }catch (Exception e){
                e.printStackTrace();
            }

            System.out.println(address + " - Respondi ao get do cliente " + a.port());
            return CollectionSerializer.getObjectInByte(response);
        }, es);

    }


    /*
     * Handler que trata de pedidos de servidor nomeadamente de pedidos para obter o valor de um dado conjunto de chaves
     * em que é assumido que este é o responsavel pela chaves enviadas
     */
    private void ServerGetHandler(){
        ms.registerHandler("getServer", (a, m)-> {
            System.out.println(address +" - Recebi um get de um Server");
            PeerDataGet dados = null;
            Map<Long, byte[]> resp = null;
            try {

                dados = (PeerDataGet) CollectionSerializer.getObjectFromByte(m);

                resp = new HashMap<>();
                if(this.clock.teste()){
                    System.out.println("BLOQUEOU O LOCK");
                }
                this.clock.lock();


            }catch (Exception e){e.printStackTrace();}

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

    /*
     * Handler que trata de pedidos de servidores nomeadamente pedidos para dar update ao clock visto que
     * um servidor pode mandar mensagem para qualquer peer e tem de manter os restantes informados sobre as mensagens que envia
     * de modo a nao bloquear quando mandar uma mensagem para os restantes
     */
    private void ClockUpdateHandler(){
        ms.registerHandler("updateClock", (a, m)-> { //m  é lista de inteiros
            List<Integer> tag = (List<Integer>) CollectionSerializer.getObjectFromByte(m);
            this.clock.lock();
            this.clock.updateVectorClock(tag);
            System.out.println(address +" - Recebi um updateClock - Vetor atualiazdo " + this.clock.getVector());
            checkMsgQueue();
            this.clock.unLock();

        }, es);

    }


    /*
     * Funçao auxiliar que escreve uma lista de pares, chave-valor, na hashmap local
     * É assumido que a concorrencia a hashmap e das chaves ja foi tratada antes ou nao é um problema
     */
    private void writeKeysInHashMap(List< Pair<Long, byte[]>> lista){
        if(lista.size()==0)return;
        for(Pair<Long, byte[]> entry : lista){
            keysValues.put(entry.getKey(),new KeyValue(entry.getValue()));
        }
        System.out.println(address +" - ESCREVI CHAVES NA HASHMAP");

    }


    /*
     * Funçao auxiliar que dá lock a um lista de pares, chave-valor, na hashmap local
     */
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
    }

    /*
     * Funçao auxiliar que dá unlock a um lista de pares, chave-valor, na hashmap local
     */
    private void unlockKeys(List<Pair<Long, byte[]>> lista){
        if(lista.size()==0)return;
        this.hashMapLock.lock();
        for(Pair<Long, byte[]> entry: lista){
            keysValues.get(entry.getKey()).unLock();
        }
        this.hashMapLock.unlock();
    }



    /*
     * Funçao auxiliar que tem como input um Map, Server Destino-Lista de chaves, que vai enviar para o server com o menor port
     * todas as chaves, i.e, as chaves que este é responsavel mais as que nenhum dos dois é responsavel.
     * O server destino é depois responsavel por enviar o resto das chaves para o seu responsavel
     */
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
                    try {
                        System.out.println(this.address + " - Mensagem putServer enviada para peer " + finalMinSv + " com o clock " + tagclock);

                    }catch (Exception e){e.printStackTrace();}

                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                });

        checkMsgQueue();
        sendTagsToAllPeers(minSv,tagclock);

        }catch (Exception e){
            e.printStackTrace();
        }
        //System.out.println("AQUIIIIIIII");
    }


    /*
     * Funçao auxiliar que manda tags de updates de clock para os outro peers menos para o proprio servidor e para o "minSv"
     * que corresponde ao servidor a quem ja enviou uma mensagem com dados
     */
    private void sendTagsToAllPeers(int minSv,List<Integer> tagclock){
        for(int i = 0; i< peers.size();i++){
            if(peers.get(i)== minSv || peers.get(i) == address) continue;
            ms.sendAsync(Address.from("localhost", peers.get(i)), "updateClock", CollectionSerializer.getObjectInByte(tagclock)).
                    exceptionally(e -> {
                        e.printStackTrace();
                        return null;
                    });
        }
    }



    /*
     * Funçao auxiliar que insere na hashmap de input,response, os valores que tem na hashmap local que corresponde aos que estao na lista de input
     */
    private void getKeysOfHashMap(ArrayList<Pair<Long, byte[]>> lista,Map<Long,byte[]> response){
        for(Pair<Long, byte[]> entry : lista){
            response.put(entry.getKey(),keysValues.get(entry.getKey()).getDados());
        }

    }

    /*
     * Funçao auxiliar que envia pedidos de chaves para o servidor responsavel por elas de modo a obter o valor atual das mesmas
     */
    private void sendGetRequestToRespectivSv(Map<Integer, List<Long>> map,Map<Long,byte[]> response) throws Exception{
        if(map.keySet().isEmpty()) {
            return;

        }

        List<Integer> tagclock = clock.incAndGetVectorClone();

        System.out.println(address +" - Get - tag do clock que envio " + tagclock);


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



    /*
     * Funçao auxiliar verifica as mensagens que estao na msgqueue,ou seja mensagens que nao verificaram a regra causal quando foram recebidas,
     * e volta a testar a causualiadde das mesmas para verificar se ja podem ser respondidas
     */
    private void checkMsgQueue(){
        boolean respostaCausal;

        this.queueLock.lock();
        try {
        Iterator<PeerDataPut> iterator = msgQueue.iterator();
        while(iterator.hasNext()){
            PeerDataPut data = iterator.next();
            respostaCausal = clock.regraCausal(data.getVectorTag(), getVecPosition(data.getSender()));
            if(respostaCausal){
                clock.updateVectorClock(data.getVectorTag());
                lockKeys(data.getList().getLista());
                System.out.println(address + " - Mensagem na MsgQueue lida");
                sleep(200);
                writeKeysInHashMap(data.getList().getLista());
                sendKeysToRespectiveSv(data.getOtherData());

                unlockKeys(data.getList().getLista());
                iterator.remove();
                msgQueue.remove(data);
            }

        }
        }catch (Exception e){
            e.printStackTrace();
        }
        this.queueLock.unlock();

    }

    /*
     * Funçao auxiliar que dado um port de um servidor verifica em que posiçao da lista,peers, este se encontra
     */
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
