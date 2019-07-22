package com.bia.sandbox.mudlog;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;

public interface LithologyDao {

    public void save(CsLithology csLithology, CsGeologyInterval csGeologyInterval);

}
