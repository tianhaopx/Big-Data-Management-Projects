package data;

import java.io.*;
import java.security.SecureRandom;
import java.util.Random;

public class GenerateTransactionData {
    static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static SecureRandom rnd = new SecureRandom();

    public String randomCustID() {
        Random length = new Random();
        int randomCustID = length.nextInt((50000 - 1) + 1) + 1;
        return Integer.toString(randomCustID);
    }

    public String randomTransTotal() {
        Random length = new Random();
        float randomTransTotal = length.nextFloat() * (1000.0f - 10.0f) + 1.0f + 10.0f;
        return Float.toString(randomTransTotal);
    }

    public String randomNumItems() {
        Random length = new Random();
        int randomNumItems = length.nextInt((10 - 1) + 1) + 1;
        return Integer.toString(randomNumItems);
    }

    public String radomDesc() {
        int len = randomLength();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    public Integer randomLength() {
        Random length = new Random();
        int randomLength = length.nextInt((50 - 20) + 1) + 20;
        return randomLength;
    }

    public String randomTransactionInstance(int n) {
        String id = Integer.toString(n);
        String CustID = randomCustID();
        String TransTotal = randomTransTotal();
        String NumItems = randomNumItems();
        String Desc = radomDesc();
        StringBuilder sb = new StringBuilder().append(id).append(',').append(CustID).append(',').append(TransTotal).append(',').append(NumItems).append(',').append(Desc);
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        GenerateTransactionData a = new GenerateTransactionData();
        File fout = new File("transcation");
        FileOutputStream fos = new FileOutputStream(fout);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        for (int i = 1; i <= 5000000; i++) {
            bw.write(a.randomTransactionInstance(i));
            bw.newLine();
        }
        bw.close();
    }

}
