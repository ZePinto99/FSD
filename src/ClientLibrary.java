import Data.PeerDataPut;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ClientLibrary {
    private NettyMessagingService ms;
    private List<Integer> nodes;
    private int peer;

    private ClientLibrary(){}

    public ClientLibrary(List<Integer> nodes,int adress){
        this.nodes = nodes;
        this.ms = new NettyMessagingService("cliente", Address.from(adress), new MessagingConfig());
        peer = chooseRandomPeerPort();
        ms.start();
    }

    private int chooseRandomPeerPort(){
        Random rand = new Random();
        //return rand.nextInt(nodes.size());
        return 12346;
    }



    public CompletableFuture<Void> put(Map<Long,byte[]> values){
        return ms.sendAsync(Address.from("localhost", peer), "put", CollectionSerializer.getObjectInByte(values))
                .thenRun(() -> {
                    System.out.println("Mensagem put enviada!");
                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                });


    }


    // TODO: isto ainda nao está bem implementado
    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
        byte[] collection = CollectionSerializer.getObjectInByte(keys);


        return ms.sendAndReceive(Address.from("localhost",peer ), "get", collection).thenApply(bytes -> {
            System.out.println("Mensagem enviada para " + peer + " e recebida");
            return (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(bytes);

        }).exceptionally(e -> {
            e.printStackTrace();
            return null;
        });
    }


    // serve apenas para testar causualidade de mensagens
    public CompletableFuture<Void> teste(PeerDataPut pd){

        return ms.sendAsync(Address.from("localhost", 12345), "putServer", CollectionSerializer.getObjectInByte(pd))
                .thenRun(() -> {
                    System.out.println("Mensagem put enviada!");
                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                });
    }







}
