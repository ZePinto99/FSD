public class Main {

    public static void main(String[] args) {
        for (int add = 12345; add<12348; add++) {
            int finalAdd = add;
            Thread peer = new Thread(() -> {
                Server s = new Server(finalAdd);
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
