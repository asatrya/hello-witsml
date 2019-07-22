package com.bia.sandbox;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.LithostratigraphyStruct;

public interface MudLogLithostatigraphicDao {

    public void save(LithostratigraphyStruct lithostratigraphyStruct, CsGeologyInterval csGeologyInterval);

}
