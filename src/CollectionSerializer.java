import java.io.*;

public class CollectionSerializer {

    /*
     * Funçao que faz a Serialização de um objeto
     */
    public static byte[] getObjectInByte(Object object) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            objectOutputStream.close();
            // get the byte array of the object
            byte[] obj= byteArrayOutputStream.toByteArray();
            byteArrayOutputStream.close();
            return obj;
        }catch(IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /*
     * Funçao que faz a Deserialização de um objeto
     */
    public static Object getObjectFromByte(byte[] bytes) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            Object tmp =  objectInputStream.readObject();
            byteArrayInputStream.close();
            return tmp;

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
