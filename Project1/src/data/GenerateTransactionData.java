package data;

import java.io.*;
import java.util.Random;

public class GenerateTransactionData {
    static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

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

    public String randomDesc() {
        Random length = new Random();
        int randomLength = length.nextInt((50 - 20) + 1) + 10;
        String sb = "";
        for (int i = 0; i < randomLength; i++)
            sb += AB.charAt(length.nextInt(AB.length()));
        return sb;
    }

    public String randomTransactionInstance(int n) {
        String id = Integer.toString(n);
        String CustID = randomCustID();
        String TransTotal = randomTransTotal();
        String NumItems = randomNumItems();
        String Desc = randomDesc();
        String str = id + ',' + CustID + ',' + TransTotal + ',' + NumItems + ',' + Desc;
        return str;
    }

    public static void main(String[] args) throws IOException {
        GenerateTransactionData a = new GenerateTransactionData();
        File fout = new File("Project1/input/transaction");
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int i = 1; i <= 5000000; i++) {
            bw.write(a.randomTransactionInstance(i));
            bw.newLine();
        }
        bw.close();
    }
}
