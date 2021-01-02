import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Teste {
    public static void main(String[] args) throws Exception {
        try {
            //establecer ligação
            System.out.println("Quer falar com que servidor?");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            Socket socket = new Socket("127.0.0.1", Integer.parseInt(in.readLine())+10);
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            System.out.println("Falei com o servidor");

            //informar do destino
            System.out.println("key,value");
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
