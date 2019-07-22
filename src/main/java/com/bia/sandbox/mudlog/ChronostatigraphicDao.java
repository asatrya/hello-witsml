package com.bia.sandbox.mudlog;

import com.hashmapinc.tempus.WitsmlObjects.v1411.ChronostratigraphyStruct;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;

public interface ChronostatigraphicDao {

    public void save(ChronostratigraphyStruct chronostratigraphyStruct, CsGeologyInterval csGeologyInterval);

}
