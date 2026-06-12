/*
 * VolTraderBridgePlugin.java — companion plugin for vol_trader_v15.html
 * ─────────────────────────────────────────────────────────────────────────
 * Serves your live GE fills to the terminal over a tiny local HTTP server.
 * ZERO external dependencies: uses the JDK's built-in com.sun.net.httpserver
 * and RuneLite's bundled Gson. No WebSocket library needed.
 *
 * HOW TO BUILD (one-time, ~15 minutes):
 *   1. Clone the official plugin template:
 *        https://github.com/runelite/example-plugin
 *      (see also the Developer Guide: https://github.com/runelite/runelite/wiki/Developer-Guide)
 *   2. Drop this file into src/main/java/com/voltrader/ and fix the package
 *      line below to match.
 *   3. Run the ExamplePluginTest main class from IntelliJ — RuneLite starts
 *      with the plugin sideloaded. Enable "VolTrader Bridge" in the plugin
 *      list.
 *   4. In the terminal, click BRIDGE in the header. It should flip to
 *      BRIDGE LIVE within a few seconds.
 *
 * For reference on richer GE-event handling (margin checking, slot state
 * machines), study Flipping Utilities — it consumes the same event:
 *   https://github.com/Flipping-Utilities/rl-plugin
 *
 * PROTOCOL (matches the terminal's poller):
 *   GET http://localhost:8087/trades?after=<ts>
 *   → JSON array of { ts, slot, state, itemId, quantitySold, price, spent }
 *   Only completed offers (BOUGHT / SOLD) are recorded; partial fills are
 *   reported by RuneLite as BUYING/SELLING and ignored here, so each row
 *   is a final, settled fill — exactly what the FIFO ledger wants.
 */
package com.voltrader;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import net.runelite.api.GrandExchangeOfferState;
import net.runelite.api.events.GrandExchangeOfferChanged;
import net.runelite.client.eventbus.Subscribe;
import net.runelite.client.plugins.Plugin;
import net.runelite.client.plugins.PluginDescriptor;

import javax.inject.Inject;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@PluginDescriptor(
    name = "VolTrader Bridge",
    description = "Serves completed GE fills to the VOL TRADER terminal on localhost:8087"
)
public class VolTraderBridgePlugin extends Plugin
{
    private static final int PORT = 8087;
    private static final int MAX_TRADES = 2000;   // ring-buffer cap

    @Inject
    private Gson gson;

    private HttpServer server;
    private final List<Trade> trades = new CopyOnWriteArrayList<>();

    /** Lightweight payload row — field names must match the terminal. */
    private static class Trade
    {
        long ts;            // wall-clock millis, used as the ?after= cursor
        int slot;
        String state;       // "BOUGHT" | "SOLD"
        int itemId;
        int quantitySold;
        int price;          // offer price per unit
        long spent;         // total gp moved (RuneLite's getSpent())
    }

    @Override
    protected void startUp() throws Exception
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", PORT), 0);
        server.createContext("/trades", exchange ->
        {
            // CORS: the terminal runs from file:// (origin "null"); '*' covers it
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "application/json");

            long after = 0L;
            String q = exchange.getRequestURI().getQuery();
            if (q != null && q.startsWith("after="))
            {
                try { after = Long.parseLong(q.substring(6)); }
                catch (NumberFormatException ignored) { }
            }

            List<Trade> fresh = new ArrayList<>();
            for (Trade t : trades)
            {
                if (t.ts > after)
                {
                    fresh.add(t);
                }
            }

            byte[] body = gson.toJson(fresh).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body);
            }
        });
        server.setExecutor(null);   // single default executor is plenty
        server.start();
    }

    @Override
    protected void shutDown()
    {
        if (server != null)
        {
            server.stop(0);
            server = null;
        }
        trades.clear();
    }

    @Subscribe
    public void onGrandExchangeOfferChanged(GrandExchangeOfferChanged event)
    {
        // Ignore empty-slot resets
        if (event.getOffer().getItemId() == 0)
        {
            return;
        }

        GrandExchangeOfferState state = event.getOffer().getState();

        // Only final fills — the terminal's FIFO ledger wants settled trades
        if (state != GrandExchangeOfferState.BOUGHT
            && state != GrandExchangeOfferState.SOLD)
        {
            return;
        }

        Trade t = new Trade();
        t.ts = System.currentTimeMillis();
        t.slot = event.getSlot();
        t.state = state.name();
        t.itemId = event.getOffer().getItemId();
        t.quantitySold = event.getOffer().getQuantitySold();
        t.price = event.getOffer().getPrice();
        t.spent = event.getOffer().getSpent();
        trades.add(t);

        // Trim the ring buffer
        while (trades.size() > MAX_TRADES)
        {
            trades.remove(0);
        }
    }
}
