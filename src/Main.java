import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        List<Integer> servers = new ArrayList<>();
        servers.add(12345);
        servers.add(12346);
        servers.add(12347);
        servers.add(8000);

        for (int add = 12345; add<12348; add++) {
            int finalAdd = add;
            Thread peer = new Thread(() -> {
                Server s = new Server(servers,finalAdd);
                try {
                    s.startServer();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            peer.start();
        }


    }
}
