import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
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

        //Intoduzir um ip do server
        System.out.println("Insert my address: (12345-12348)");
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        int address = Integer.parseInt(input.readLine());

        //Ver qual é a posição do server no relógio vetorial
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

        //mensagem recebida de outro servidor
        ms.registerHandler("chat", (a, m)-> {
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
                case 12348:
                    messagePosition = 3;
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

        ms.registerHandler("resend", (ra, resend)->{
            String messageRende = new String(resend);
       /*     ms.sendAsync(Address.from("localhost", ra.port()), "chat", (new String(resend)).getBytes())
                    .thenRun(() -> {
                        System.out.println("Recebido pedido de reenvio!");
                    })
                    .exceptionally(t -> {
                        t.printStackTrace();
                        return null;
                    });*/ System.out.println("Recebido pedido de reenvio!");
        }, es);

        int finalVectorPosition1 = vectorPosition;
        ms.registerHandler("put", (a, m)-> {
            String messageRecive = new String(m);
            //hash da mensagem
            //ver se é para mim ou não
            String[] key_value = messageRecive.split(",");
            //se for para mim guardo e atualizo o relógio
            if (Integer.parseInt(key_value[0]) == finalVectorPosition1){
                System.out.println("Guardei a mensagem");
            }
            else System.out.println("A mensagem não é para mim");

                //
            System.out.println("Recebi um put");
                }, es);

        ms.registerHandler("get", (a, m)-> {
            System.out.println("Recebi um get");
        }, es);



        ms.start();

        //Caso receba uma mensagem do cliente
        ServerSocket client_listenner = new ServerSocket(address+10);
        Thread listener = new Thread(() -> {
            try {
                System.out.println("##");
                Socket s = client_listenner.accept();
                System.out.println("New client");
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String message_client = null;
                //Quando recebe mensagem do cliente
                while((message_client = in.readLine()) != null){
                    System.out.println("Client message");
                    String operation_keyvalue[] = message_client.split(";");
                    //put ou get?
                        /*///para mim ou para outro?  (!!em falta!!)
                            //se para mim fazer a operação
                            //se para outro enviar mensagem*/
                        System.out.println(operation_keyvalue[0]);
                        if (operation_keyvalue[0].equals("put")){
                            //envia para os outros
                            operation_keyvalue[1] = vector.toString() + ";"  + operation_keyvalue[1];
                            for (int i = 12345; i < 12347; i++) {
                                if(i != address){ //para não enviar para ele próprio
                                    ms.sendAsync(Address.from("localhost", i), "put", operation_keyvalue[1].getBytes())
                                            .thenRun(() -> {
                                                System.out.println("Mensagem put enviada!");
                                            })
                                            .exceptionally(t -> {
                                                t.printStackTrace();
                                                return null;
                                            });
                                }
                            }
                        } else{
                            //envia para os outros
                            operation_keyvalue[1] = vector.toString() + ";"  + operation_keyvalue[1];
                            for (int i = 12345; i < 12347; i++) {
                                if(i != address){ //para não enviar para ele próprio
                                    ms.sendAsync(Address.from("localhost", i), "get", operation_keyvalue[1].getBytes())
                                            .thenRun(() -> {
                                                System.out.println("Mensagem get enviada!");
                                            })
                                            .exceptionally(t -> {
                                                t.printStackTrace();
                                                return null;
                                            });
                                }
                            }
                        }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        listener.start();

        //enviar mensagem para servidor
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
        }catch (IOException ignore) {}
    }
}
