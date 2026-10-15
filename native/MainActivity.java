package com.mo2tamarati.app;

import android.os.Bundle;

import com.getcapacitor.BridgeActivity;

public class MainActivity extends BridgeActivity {
    @Override
    public void onCreate(Bundle savedInstanceState) {
        registerPlugin(LanHostPlugin.class);
        super.onCreate(savedInstanceState);
    }
}
