package com.bia.sandbox;

import com.hashmapinc.tempus.WitsmlObjects.v1411.ChronostratigraphyStruct;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;

public interface MudLogChronostatigraphicDao {

    public void save(ChronostratigraphyStruct chronostratigraphyStruct, CsGeologyInterval csGeologyInterval);

}
