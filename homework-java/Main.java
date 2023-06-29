public class Main {
    public static void main(String[] args){
        int n = 1;
        for(int i=0; i<args.length; ++i) {
            switch(i){
                case 0:
                n = Integer.parseInt(args[i]);
                break;
                default:
                n = n  * Integer.parseInt(args[i]);
                break;
            }
        }

        if(n % 2 == 0){
            System.out.println(n + " - Genap");
        }else{
            System.out.println(n + " - Ganjil");
        }
    }
}