package com.bia.sandbox;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

public interface MudLogGeologyIntervalDao {

    public void save(CsGeologyInterval csGeologyInterval, ObjMudLog objMudLog);

}
