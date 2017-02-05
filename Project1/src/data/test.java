package data;

import java.util.Random;

/**
 * Created by tianhao on 2/5/17.
 */
public class test {
    private static Random random = new Random();

    public static float randomInRange(float min, float max) {
        float range = max - min;
        float scaled = random.nextFloat() * range;
        float shifted = scaled + min;
        return shifted;
    }

    public static void main(String[] args) {
        float min = 100f;
        float max = 100f;
        for (int i = 0; i < 5000000; i++) {
            float num = randomInRange(10f, 1000f);
            if (num < min)
                min = num;
            if (num > max)
                max = num;
        }
        System.out.println(min + ", " + max);
    }
}
