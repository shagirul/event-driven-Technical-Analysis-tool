package dev.team.autotrade.tick;

public class DesiredEvent {
    public int ver;
    public String sessionId;
    public String symbol;
    public String op;   // "SUB" | "UNSUB"
    public long ts;

    public DesiredEvent() {}
}
