import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Client {
    public static void main(String[] args) throws Exception {
       /* ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

        //Intoduzir um ip do server
        int address = 5555;

        NettyMessagingService ms = new NettyMessagingService("nome", Address.from(address), new MessagingConfig());

        //mensagem recebida
        ms.registerHandler("reply", (a, m)-> {
            //Tratamento do vetor e mensagem
            String messageRecive = new String(m);
            String delims = ";";
            String[] tokens = messageRecive.split(delims);
            String tokenAux = tokens[0].substring(1, 8).replace(" ", "");
            String[] messageVector = tokenAux.split(",");
            System.out.println("Obtive resposta");

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
                    });*/ /*
        }, es);

        ms.registerHandler("put", (a, m)-> {
            String messageRecive = new String(m);
            //hash da mensagem
            //ver se é para mim ou não
            //se for para mim guardo e atualizo o relógio
            //
        }, es);

        ms.registerHandler("get", (a, m)-> {

        }, es);



        ms.start();

        System.out.println("0-get 1-put");
        int option = Integer.parseInt(input.readLine());
        String message = null;

        try {
            while ((message = input.readLine()) != ".") {
                for (int i = 12345; i < 12347; i++) {
                    if(option == 0){ //para não enviar para ele próprio
                        ms.sendAsync(Address.from("localhost", i), "get", message.getBytes())
                                .thenRun(() -> {
                                    System.out.println("Mensagem enviada!");
                                })
                                .exceptionally(t -> {
                                    t.printStackTrace();
                                    return null;
                                });
                    }
                    else {
                        ms.sendAsync(Address.from("localhost", i), "put", message.getBytes())
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
        try {
            //establecer ligação
            System.out.println("Quer falar com que servidor?");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            Socket socket = new Socket("127.0.0.1", Integer.parseInt(in.readLine())+10);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            System.out.println("Falei com o servidor");

            //informar do destino
            System.out.println("destino;key,value");
            String buffer;
            while ((buffer = in.readLine()) != null) {
                out.println(buffer);

                out.flush();
            }

            //fechar
            socket.shutdownOutput();
            socket.shutdownInput();
            socket.close();
        } catch (IOException ignored) {}
    }
}
