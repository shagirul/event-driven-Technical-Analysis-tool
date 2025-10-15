package dev.team.autotrade.tick;

public class Tick {
    public String symbol;
    public long ts;
    public double price;
    public long size;
    public Tick() {}
    public Tick(String symbol, long ts, double price, long size) {
        this.symbol = symbol; this.ts = ts; this.price = price; this.size = size;
    }
}
