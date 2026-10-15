package com.mo2tamarati.app;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/** Tiny HTTP + Server-Sent-Events server used to share one conference live on the local network. */
public class LanServer {
    public interface Host {
        byte[] asset(String path) throws IOException;
        void onEdit(String json);
        void onClients(String json, int count);
    }

    private static class Client {
        final Socket sock; final OutputStream out; final String token; final String id; final String name;
        Client(Socket s, OutputStream o, String t, String i, String n) { sock = s; out = o; token = t; id = i; name = n; }
    }

    private final Host host;
    private ServerSocket server;
    private volatile boolean running;
    private volatile String viewToken = "";
    private volatile boolean allowAll;
    private final Set<String> allowed = ConcurrentHashMap.newKeySet();
    private final Set<String> asked = ConcurrentHashMap.newKeySet();
    private volatile String state = "{}";
    private volatile int version = 0;
    private final List<Client> clients = new CopyOnWriteArrayList<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private ScheduledExecutorService ka;

    public LanServer(Host h) { host = h; }

    public void config(String vt) { if (vt != null) viewToken = vt; }

    /** Host grants or revokes edit permission for one device (by its client id). */
    public synchronized void setAllow(String id, boolean on) {
        if (id == null || id.isEmpty()) return;
        if (on) { allowed.add(id); asked.remove(id); } else allowed.remove(id);
        version++; broadcast("host"); notifyClients();
    }

    /** Host grants or revokes edit permission for every device. */
    public synchronized void setAllowAll(boolean on) {
        allowAll = on;
        if (on) asked.clear();
        version++; broadcast("host"); notifyClients();
    }

    private void notifyClients() { host.onClients(clientsJson(), clients.size()); }

    private String clientsJson() {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Client c : clients) {
            if (!first) sb.append(',');
            first = false;
            sb.append("{\"id\":\"").append(esc(c.id)).append("\",\"name\":\"").append(esc(c.name))
              .append("\",\"edit\":").append(allowAll || allowed.contains(c.id))
              .append(",\"req\":").append(asked.contains(c.id)).append('}');
        }
        return sb.append(']').toString();
    }

    private static String esc(String s) {
        StringBuilder o = new StringBuilder();
        for (char ch : s.toCharArray()) {
            if (ch == '"' || ch == '\\') o.append('\\').append(ch);
            else if (ch < 32) o.append(' ');
            else o.append(ch);
        }
        return o.toString();
    }

    public synchronized void setState(String json) {
        if (json == null || json.isEmpty()) return;
        state = json; version++; broadcast("host");
    }

    public int start(int port) throws IOException {
        IOException last = null;
        for (int p = port; p < port + 10; p++) {
            try {
                ServerSocket s = new ServerSocket();
                s.setReuseAddress(true);
                s.bind(new InetSocketAddress(p));
                server = s; running = true;
                pool.execute(this::acceptLoop);
                ka = Executors.newSingleThreadScheduledExecutor();
                ka.scheduleAtFixedRate(this::keepAlive, 15, 15, TimeUnit.SECONDS);
                return p;
            } catch (IOException e) { last = e; }
        }
        throw last != null ? last : new IOException("no port");
    }

    public void stop() {
        running = false;
        try { if (server != null) server.close(); } catch (IOException ignored) {}
        if (ka != null) ka.shutdownNow();
        for (Client c : clients) close(c);
        clients.clear();
        pool.shutdownNow();
    }

    private void acceptLoop() {
        while (running) {
            try {
                final Socket s = server.accept();
                s.setTcpNoDelay(true);
                pool.execute(() -> handle(s));
            } catch (IOException e) { if (!running) break; }
        }
    }

    /** One QR for everyone: everybody joins as "view"; the host upgrades devices one by one (or all). */
    private String perm(String k, String id) {
        if (k == null || k.isEmpty() || viewToken.isEmpty() || !k.equals(viewToken)) return null;
        return (allowAll || (id != null && !id.isEmpty() && allowed.contains(id))) ? "edit" : "view";
    }

    private void handle(Socket s) {
        boolean keep = false;
        try {
            s.setSoTimeout(15000);
            InputStream in = new BufferedInputStream(s.getInputStream());
            OutputStream out = s.getOutputStream();
            ByteArrayOutputStream hb = new ByteArrayOutputStream();
            int b, st = 0;
            while ((b = in.read()) != -1) {
                hb.write(b);
                st = (b == '\r' && (st == 0 || st == 2)) ? st + 1 : (b == '\n' && (st == 1 || st == 3)) ? st + 1 : 0;
                if (st == 4) break;
                if (hb.size() > 16384) throw new IOException("header too large");
            }
            String head = hb.toString("ISO-8859-1");
            String[] lines = head.split("\r\n");
            if (lines.length == 0 || lines[0].isEmpty()) return;
            String[] rl = lines[0].split(" ");
            if (rl.length < 2) return;
            String method = rl[0], target = rl[1];
            int len = 0;
            for (int i = 1; i < lines.length; i++) {
                int c = lines[i].indexOf(':');
                if (c > 0 && lines[i].substring(0, c).trim().equalsIgnoreCase("content-length"))
                    len = Integer.parseInt(lines[i].substring(c + 1).trim());
            }
            String path = target, query = "";
            int q = target.indexOf('?');
            if (q >= 0) { path = target.substring(0, q); query = target.substring(q + 1); }
            Map<String, String> qs = parseQuery(query);

            if (method.equals("OPTIONS")) { send(out, 204, "text/plain", new byte[0]); return; }

            if (method.equals("GET") && path.equals("/events")) {
                String pm = perm(qs.get("k"), qs.get("id"));
                if (pm == null) { send(out, 403, "text/plain", "forbidden".getBytes()); return; }
                String cid = qs.getOrDefault("id", "");
                String cname = qs.getOrDefault("name", "").trim();
                if (cname.length() > 30) cname = cname.substring(0, 30);
                if (cname.isEmpty()) cname = "جهاز";
                for (Client old : clients) if (old.id.equals(cid)) { clients.remove(old); close(old); }
                String h = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n"
                        + cors() + "\r\n";
                out.write(h.getBytes(StandardCharsets.UTF_8));
                s.setSoTimeout(0);
                Client c = new Client(s, out, qs.get("k"), cid, cname);
                clients.add(c);
                write(c, msg(pm, "host", true));
                notifyClients();
                keep = true;
                return;
            }
            if (method.equals("POST") && path.equals("/req")) {
                String pm = perm(qs.get("k"), qs.get("id"));
                String rid = qs.getOrDefault("id", "");
                if (pm == null || rid.isEmpty()) { send(out, 403, "text/plain", "forbidden".getBytes()); return; }
                if (pm.equals("view")) { asked.add(rid); notifyClients(); }
                send(out, 200, "application/json", "{}".getBytes());
                return;
            }
            if (method.equals("GET") && path.equals("/state")) {
                String pm = perm(qs.get("k"), qs.get("id"));
                if (pm == null) { send(out, 403, "text/plain", "forbidden".getBytes()); return; }
                send(out, 200, "application/json; charset=utf-8", msg(pm, "host", false).getBytes(StandardCharsets.UTF_8));
                return;
            }
            if (method.equals("POST") && path.equals("/edit")) {
                String pm = perm(qs.get("k"), qs.get("id"));
                if (pm == null || !pm.equals("edit")) { send(out, 403, "text/plain", "forbidden".getBytes()); return; }
                if (len <= 0 || len > 5_000_000) { send(out, 400, "text/plain", "bad length".getBytes()); return; }
                byte[] body = new byte[len];
                int off = 0;
                while (off < len) { int n = in.read(body, off, len - off); if (n < 0) break; off += n; }
                String json = new String(body, 0, off, StandardCharsets.UTF_8);
                synchronized (this) { state = json; version++; broadcast(qs.getOrDefault("id", "x")); }
                host.onEdit(json);
                send(out, 200, "application/json", ("{\"v\":" + version + "}").getBytes());
                return;
            }
            if (method.equals("GET")) {
                String p = path.equals("/") ? "/index.html" : path;
                if (p.contains("..")) { send(out, 400, "text/plain", "bad".getBytes()); return; }
                try {
                    byte[] data = host.asset("public" + p);
                    send(out, 200, mime(p), data);
                } catch (IOException e) { send(out, 404, "text/plain", "not found".getBytes()); }
                return;
            }
            send(out, 405, "text/plain", "method not allowed".getBytes());
        } catch (Exception ignored) {
        } finally {
            if (!keep) { try { s.close(); } catch (IOException ignored) {} }
        }
    }

    private String msg(String perm, String by, boolean sse) {
        String json = "{\"v\":" + version + ",\"perm\":\"" + perm + "\",\"by\":\"" + by.replace("\"", "") + "\",\"state\":" + state + "}";
        return sse ? "data: " + json + "\n\n" : json;
    }

    private void broadcast(String by) {
        for (Client c : clients) {
            String pm = perm(c.token, c.id);
            if (pm == null) pm = "view";
            write(c, msg(pm, by, true));
        }
    }

    private void write(Client c, String data) {
        try {
            synchronized (c) { c.out.write(data.getBytes(StandardCharsets.UTF_8)); c.out.flush(); }
        } catch (IOException e) {
            clients.remove(c); close(c); notifyClients();
        }
    }

    private void keepAlive() {
        for (Client c : clients) write(c, ": ka\n\n");
    }

    private void close(Client c) { try { c.sock.close(); } catch (IOException ignored) {} }

    private static String cors() {
        return "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: content-type\r\n"
                + "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Private-Network: true\r\n";
    }

    private static void send(OutputStream out, int code, String type, byte[] body) throws IOException {
        String text = code == 200 ? "OK" : code == 204 ? "No Content" : code == 403 ? "Forbidden" : code == 404 ? "Not Found" : "Error";
        String h = "HTTP/1.1 " + code + " " + text + "\r\nContent-Type: " + type + "\r\nContent-Length: " + body.length
                + "\r\nCache-Control: no-store\r\nConnection: close\r\n" + cors() + "\r\n";
        out.write(h.getBytes(StandardCharsets.UTF_8));
        out.write(body);
        out.flush();
    }

    private static String mime(String p) {
        if (p.endsWith(".html")) return "text/html; charset=utf-8";
        if (p.endsWith(".js")) return "application/javascript";
        if (p.endsWith(".json") || p.endsWith(".webmanifest")) return "application/json";
        if (p.endsWith(".png")) return "image/png";
        return "application/octet-stream";
    }

    private static Map<String, String> parseQuery(String q) {
        Map<String, String> m = new HashMap<>();
        for (String kv : q.split("&")) {
            if (kv.isEmpty()) continue;
            int i = kv.indexOf('=');
            try {
                String k = URLDecoder.decode(i < 0 ? kv : kv.substring(0, i), "UTF-8");
                String v = i < 0 ? "" : URLDecoder.decode(kv.substring(i + 1), "UTF-8");
                m.put(k, v);
            } catch (UnsupportedEncodingException ignored) {}
        }
        return m;
    }

    /** Private IPv4 addresses of this device (Wi-Fi first, then hotspot, then others). */
    public static List<String> localIps() {
        List<String> wifi = new ArrayList<>(), other = new ArrayList<>();
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (InetAddress a : Collections.list(ni.getInetAddresses())) {
                    if (a instanceof Inet4Address && a.isSiteLocalAddress()) {
                        String n = ni.getName().toLowerCase();
                        (n.startsWith("wlan") || n.startsWith("ap") || n.startsWith("swlan") ? wifi : other).add(a.getHostAddress());
                    }
                }
            }
        } catch (SocketException ignored) {}
        wifi.addAll(other);
        return wifi;
    }
}
