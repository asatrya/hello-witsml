package com.bia.sandbox;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsMudLogParameter;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

public interface MudLogParameterDao {

    public void save(CsMudLogParameter csMudLogParameter, ObjMudLog objMudLog);

}
