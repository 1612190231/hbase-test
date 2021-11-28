package com.luck.utils;

/**
 * @author luchengkai
 * @description 空间填充曲线工具类
 * @date 2021/11/27 19:22
 */
public class HibertUtil {
//    void rot(int n, int *x, int *y, int rx, int ry);

    //XY坐标到Hilbert代码转换
    int xy2d (int n, int x, int y) {
        int rx, ry, s, d=0;
        for (s=n/2; s>0; s/=2) {
            rx = (x & s) > 0 ? 1 : 0;
            ry = (y & s) > 0 ? 1 : 0;
            d += s * s * ((3 * rx) ^ ry);
            int[] res = rot(s, x, y, rx, ry);
            x = res[0];
            y = res[1];
        }
        return d;
    }

    //Hilbert代码到XY坐标
    void d2xy(int n, int d, int x, int y) {
        int rx, ry, s, t=d;
        x = y = 0;
        for (s=1; s <= n/2; s*=2) {
            rx = 1 & (t/2);
            ry = 1 & (t ^ rx);
            rot(s, x, y, rx, ry);
            x += s * rx;
            y += s * ry;
            t /= 4;
        }
    }

    int[] rot(int n, int x, int y, int rx, int ry) {
        if (ry == 0) {
            if (rx == 1) {
            x = n-1 - x;
            y = n-1 - y;
            }
        //Swap x and y
            int t = x;
            x = y;
            y = t;
        }
        int[] res = new int[2];
        res[0] = x;
        res[1] = y;
        return res;
    }
}
