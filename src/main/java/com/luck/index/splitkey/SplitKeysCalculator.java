package com.luck.index.splitkey;

public interface SplitKeysCalculator {
    byte[][] calcSplitKeys();

    byte[][] getSplitKeys(int regionNum);
}
