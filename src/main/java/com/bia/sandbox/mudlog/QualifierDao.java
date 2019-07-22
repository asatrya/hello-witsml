package com.bia.sandbox.mudlog;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsQualifier;

public interface QualifierDao {

    public void save(CsQualifier csQualifier, CsLithology csLithology);

}
