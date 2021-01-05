import Data.PeerData;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ClientLibrary {
    private NettyMessagingService ms;
    private List<Integer> nodes;

    private ClientLibrary(){}

    public ClientLibrary(List<Integer> nodes){
        this.nodes = nodes;
        this.ms = new NettyMessagingService("cliente", Address.from(8000), new MessagingConfig());
        ms.start();
    }

    // TODO: implementar isto direito a escolher a sorte um nodo da lista
    private int chooseRandomPeerPort(){
        return 12346;
    }



    public CompletableFuture<Void> put(Map<Long,byte[]> values){
        int peer =  chooseRandomPeerPort();

        return ms.sendAsync(Address.from("localhost", peer), "put", CollectionSerializer.getObjectInByte(values))
                .thenRun(() -> {
                    System.out.println("Mensagem put enviada!");
                })
                .exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                });


    }


    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
        int peer =  chooseRandomPeerPort();
        byte[] collection = CollectionSerializer.getObjectInByte(keys);


        return ms.sendAndReceive(Address.from("localhost",peer ), "get", collection).thenApply(bytes -> {
            System.out.println("Mensagem enviada para " + peer + " e recebida");
            return (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(bytes);

        }).exceptionally(e -> {
            e.printStackTrace();
            return null;
        });
    }


    public CompletableFuture<Void> teste(PeerData pd){

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
