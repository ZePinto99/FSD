import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

//Servidor com relógio vetorial
//Regra da entrega causal
//(Falta comunicar com clientes!)
public class Server {

    public static void main(String[] args) throws Exception {
        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        List<Integer> vector = new ArrayList<>(Collections.nCopies(3, 0));

        System.out.println("Insert my address: (12345-12347)");
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        int address = Integer.parseInt(input.readLine());
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

        NettyMessagingService ms = new NettyMessagingService("nome", Address.from(address), new MessagingConfig());
        int finalVectorPosition = vectorPosition;
        ms.registerHandler("chat", (a, m)-> { //Sempre que receber uma mensagem de outro candidato guarda a porta numa lista
            //Tratamento do vetor e mensagem
            String messageRecive = new String(m);
            String delims = ";";
            String[] tokens = messageRecive.split(delims);
            String tokenAux = tokens[0].substring(1, 8).replace(" ", "");
            String[] messageVector = tokenAux.split(",");

            //Qual é a posição criador da mensagem no vetor
            int messagePosition = -1;
            switch (a.port()){
                case 12345:
                    messagePosition = 0;
                    break;
                case 12346:
                    messagePosition = 1;
                    break;
                case 12347:
                    messagePosition = 2;
                    break;
                default:
                    System.out.println("Warning: Invalid message address");
                    break;
            }
            System.out.println("Local: " + vector.toString());
            System.out.println("Message: " + tokens[0]);

            //ver se a mensagem é válida
            boolean causalBool = true;
            //l[i] + 1 = r[i]
            if (vector.get(messagePosition) + 1 != Integer.parseInt(messageVector[messagePosition]))
                causalBool = false;
            for (int i = 0; i < 3; i++)
                if (i != messagePosition && vector.get(i) < Integer.parseInt(messageVector[i])) {
                    causalBool = false;
                    break;
                }
            //caso seja válida incrementar vetor local e  dar print da mensagem
            if (causalBool) {
                System.out.println("New message: " + tokens[1] + " from " + a);
                vector.set(messagePosition, Integer.parseInt(messageVector[messagePosition]));
            }
            //caso seja inválida mandar uma mensagem a pedir o reenvio
            else {
                //enviar mensagem de pedido de reenvio
                ms.sendAsync(Address.from("localhost", a.port()), "resend", tokens[0].getBytes()) //enviar devolta a parte mensagem que é preciso reenviar
                        .thenRun(() -> {
                            System.out.println("Pedido de reenvio!");
                        })
                        .exceptionally(t -> {
                            t.printStackTrace();
                            return null;
                        });
            }
        }, es);


        ms.registerHandler("resend", (ra, recend)->{ //Sempre que receber uma mensagem de outro candidato guarda a porta numa lista
            String messageRende = new String(recend);
       /*     ms.sendAsync(Address.from("localhost", ra.port()), "chat", (new String(recend)).getBytes())
                    .thenRun(() -> {
                        System.out.println("Recebido pedido de reenvio!");
                    })
                    .exceptionally(t -> {
                        t.printStackTrace();
                        return null;
                    });*/ System.out.println("Recebido pedido de reenvio!");
        }, es);

        ms.start();

        String message = null;
        try {
            while ((message = input.readLine()) != ".") {
                vector.set(vectorPosition, vector.get(vectorPosition) + 1);
                message = vector.toString() + ";"  + message;
                for (int i = 12345; i < 12347; i++) {
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
        }catch (IOException ignore) {}
    }
}
