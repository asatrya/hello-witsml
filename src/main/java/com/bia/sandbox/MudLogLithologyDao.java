package com.bia.sandbox;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;

public interface MudLogLithologyDao {

    public void save(CsLithology csLithology, CsGeologyInterval csGeologyInterval);

}
