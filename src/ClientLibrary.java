import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

public class ClientLibrary {
    NettyMessagingService ms;
    public void get(){

    }

 /*   private  void sendMessageServer(List<Integer> vector, int vectorPosition, int address, NettyMessagingService ms){
        String message = null;
        try {
            while ((message = input.readLine()) != ".") {
                vector.set(vectorPosition, vector.get(vectorPosition) + 1);
                message = vector.toString() + ";"  + message;
                for (int i = 12345; i < 12348; i++) {
                    if(i != address){ //para não enviar para ele próprio
                        ms.sendAsync(Address.from("localhost", i), "chat", message.getBytes())
                                .thenRun(() -> {
                                    System.out.println("Mensagem enviada!");
                                })
                                .exceptionally(t -> {
                                    t.printStackTrace();
                                    return null;
                                });
                    }
                }
            }
        }catch (IOException ignore) {}*/
}
