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
        return 12345;
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

        CompletableFuture<byte[]> completFut = ms.sendAndReceive(Address.from("localhost",peer ), "get", collection);

        Map<Long,byte[]> dados;

        try {
            byte[] bytes = completFut.get();
            dados = (Map<Long,byte[]>) CollectionSerializer.getObjectFromByte(bytes);
            CompletableFuture<Map<Long,byte[]>> response = new CompletableFuture<>();
            response.complete(dados);
            return response;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
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
