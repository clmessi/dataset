/**
 * $Id: mytest.java,v 1.0 2019/4/25 9:29 zhongzhenkai Exp $
 * <p>
 * Copyright 2017 Beijing Ultrapower Software Of Shanghai Co.Ltd
 */

/**
 * @author zhongzhenkai
 * @version $Id: mytest.java,v 1.1 2019/4/25 9:29 zhongzhenkai Exp $
 * Created on 2019/4/25 9:29
 */
public class mytest {
    private void myprint(int i) {
        System.out.println(i);
    }

    public void test() {
        int m = 100;
        for (int i = 2; i < Math.sqrt(m); i++) {
            for (int j = 2 * i; j < m; j += i) {
                myprint(j);
            }
        }
    }

    public static void main(String[] args) {
        new mytest().test();
    }
}