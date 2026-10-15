package com.mo2tamarati.app;

import android.view.WindowManager;

import com.getcapacitor.JSArray;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@CapacitorPlugin(name = "LanHost")
public class LanHostPlugin extends Plugin {
    private LanServer server;

    @PluginMethod
    public void start(PluginCall call) {
        try {
            if (server != null) server.stop();
            server = new LanServer(new LanServer.Host() {
                @Override public byte[] asset(String path) throws IOException {
                    InputStream in = getContext().getAssets().open(path);
                    ByteArrayOutputStream bo = new ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = in.read(buf)) > 0) bo.write(buf, 0, n);
                    in.close();
                    return bo.toByteArray();
                }
                @Override public void onEdit(String json) {
                    JSObject o = new JSObject();
                    o.put("state", json);
                    notifyListeners("edit", o);
                }
                @Override public void onClients(String json, int count) {
                    JSObject o = new JSObject();
                    o.put("count", count);
                    try { o.put("list", new JSArray(json)); } catch (Exception e) { o.put("list", new JSArray()); }
                    notifyListeners("clients", o);
                }
            });
            server.config(call.getString("viewToken", ""));
            server.setState(call.getString("state", "{}"));
            Integer req = call.getInt("port", 8080);
            int port = server.start(req == null ? 8080 : req);
            JSObject r = new JSObject();
            r.put("port", port);
            JSArray ips = new JSArray();
            for (String ip : LanServer.localIps()) ips.put(ip);
            r.put("ips", ips);
            keepAwake(true);
            call.resolve(r);
        } catch (Exception e) {
            call.reject("start failed: " + e.getMessage());
        }
    }

    @PluginMethod
    public void stop(PluginCall call) {
        if (server != null) { server.stop(); server = null; }
        keepAwake(false);
        call.resolve();
    }

    @PluginMethod
    public void setState(PluginCall call) {
        if (server != null) server.setState(call.getString("state", ""));
        call.resolve();
    }

    @PluginMethod
    public void setAllow(PluginCall call) {
        Boolean on = call.getBoolean("on", false);
        if (server != null) server.setAllow(call.getString("id", ""), on != null && on);
        call.resolve();
    }

    @PluginMethod
    public void setAllowAll(PluginCall call) {
        Boolean on = call.getBoolean("on", false);
        if (server != null) server.setAllowAll(on != null && on);
        call.resolve();
    }

    private void keepAwake(final boolean on) {
        if (getActivity() == null) return;
        getActivity().runOnUiThread(new Runnable() {
            @Override public void run() {
                if (on) getActivity().getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
                else getActivity().getWindow().clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
            }
        });
    }
}
