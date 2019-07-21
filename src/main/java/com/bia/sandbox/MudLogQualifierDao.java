package com.bia.sandbox;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsLithology;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsQualifier;

public interface MudLogQualifierDao {

    public void save(CsQualifier csQualifier, CsLithology csLithology);

}
