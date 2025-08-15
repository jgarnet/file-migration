package org.example;

import java.util.List;

public class BackoffCounter {
    private final List<Integer> periods;
    private int counter;

    public BackoffCounter(List<Integer> periods) {
        this.periods = periods;
    }

    public void increment() {
        this.counter++;
    }

    public void reset() {
        this.counter = 0;
    }

    public int getBackoff() {
        if (this.counter == 0) {
            return 0;
        }
        int index = Math.min(this.periods.size() - 1, this.counter - 1);
        return this.periods.get(index);
    }
}
