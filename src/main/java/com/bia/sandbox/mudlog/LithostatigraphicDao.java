package com.bia.sandbox.mudlog;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.LithostratigraphyStruct;

public interface LithostatigraphicDao {

    public void save(LithostratigraphyStruct lithostratigraphyStruct, CsGeologyInterval csGeologyInterval);

}
