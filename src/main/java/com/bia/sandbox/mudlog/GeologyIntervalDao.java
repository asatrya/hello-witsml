package com.bia.sandbox.mudlog;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

public interface GeologyIntervalDao {

    public void save(CsGeologyInterval csGeologyInterval, ObjMudLog objMudLog);

}
