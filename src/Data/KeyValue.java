package Data;

import java.util.concurrent.locks.ReentrantLock;

public class KeyValue {

    private byte[] dados;
    private ReentrantLock lockConta;

    public KeyValue(){
        this.lockConta = new ReentrantLock();

    }

    public KeyValue(byte[] d){
        this.dados = d;
        this.lockConta = new ReentrantLock();

    }

    public void lock(){
        this.lockConta.lock();
    }

    public void unLock(){
        this.lockConta.unlock();
    }


    public byte[] getDados() {
        return dados.clone();
    }

    public void setDados(byte[] dados) {
        this.dados = dados;
    }
}
