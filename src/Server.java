import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

//Servidor com relógio vetorial
//Regra da entrega causal
public class Server implements Runnable {
    private NettyMessagingService ms;
    private int address;
    private ScheduledExecutorService es;
    private Map keysValues;
    private VectorClock clock;

    public Server(int address){
        this.ms = new NettyMessagingService("nome", Address.from(address), new MessagingConfig());
        this.address = address;
        this.es = Executors.newScheduledThreadPool(1);
        this.keysValues = new HashMap();
        this.clock = new VectorClock(getVecPosition(address));
    }



    public void startServer() throws Exception {

        //mensagem recebida de outro servidor (
        readServerMessage();

        readResendMessage();

        readPutMessage();

        readGetMessage();

        ms.start();

        //Caso receba uma mensagem do cliente (!!!!!!!!!É preciso trocar este server socket para nio ou algo do género!!!!!!!!!!!)
        ServerSocket client_listenner = new ServerSocket(address+10);
        Thread listener = new Thread(() -> {
            readMessageClient(client_listenner);
        });
        listener.start();
    }

    private void readMessageClient(ServerSocket client_listenner){
        try {
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
                    operation_keyvalue[1] = clock.getVector().toString() + ";"  + operation_keyvalue[1];
                    clock.incrementPosition();
                    System.out.println(clock.getVector().toString());
                    for (int i = 12345; i < 12347; i++) {
                        if(i != address){ //para não enviar para ele próprio
                            ms.sendAsync(Address.from("localhost", i), "chat", operation_keyvalue[1].getBytes())
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
                    operation_keyvalue[1] = clock.getVector().toString() + ";"  + operation_keyvalue[1];
                    for (int i = 12345; i < 12347; i++) {
                        if(i != address){ //para não enviar para ele próprio
                            ms.sendAsync(Address.from("localhost", i), "chat", operation_keyvalue[1].getBytes())
                                    .thenRun(() -> {
                                        System.out.println(address + ":Mensagem get enviada!");
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
    }

  /*  private  void sendMessageServer(List<Integer> vector, int vectorPosition, int address, NettyMessagingService ms){
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
    }*/

    private int getVecPosition(int address){
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
        return vectorPosition;
    }

    //Não é necessário para o trabalho!
    private void readServerMessage(){
        ms.registerHandler("chat", (a, m)-> {
            //Tratamento do vetor e mensagem
            String messageRecive = new String(m);
            String delims = ";";
            String[] tokens = messageRecive.split(delims);
            String tokenAux = tokens[0].substring(1, 8).replace(" ", "");
            String[] messageVector = tokenAux.split(",");

            System.out.println(address + ": Local: " + clock.getVector().toString());
            System.out.println(address +": Message: " + tokens[0]);

            //ver se a mensagem é válida
            if (clock.regraCausal(messageVector)) {
                System.out.println("New message: " + tokens[1] + " from " + a);
                clock.getVector().set(clock.getVectorPosition(), Integer.parseInt(messageVector[clock.getVectorPosition()]));
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
    }

    private void readResendMessage(){
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
    }

    private void readPutMessage(){
        int finalVectorPosition1 = clock.getVectorPosition();
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

        ms.registerHandler("putServer", (a, m)-> {
            String messageRecive = new String(m);
            //hash da mensagem
            //ver se é para mim ou não
            String[] key_value = messageRecive.split(",");

            System.out.println("Recebi um put");
            System.out.println("Guardei a mensagem");
        }, es);
    }

    private void readGetMessage(){
        ms.registerHandler("get", (a, m)-> {
            System.out.println("Recebi um get");
        }, es);
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
