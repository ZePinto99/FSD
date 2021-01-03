import java.io.*;

public class CollectionSerializer {

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
