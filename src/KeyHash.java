public class KeyHash {

    public static int giveHashOfKey(Long key,int servers){
        long intervalo = Long.MAX_VALUE/servers;
        int i;

        if(key < 0) key = Math.abs(key);

        long intervaloBase = intervalo;

        for(i = 0;i < servers;i++){
            if( key <= intervalo ) return i;
            intervalo += intervaloBase;
        }

        return i;

    }

}
