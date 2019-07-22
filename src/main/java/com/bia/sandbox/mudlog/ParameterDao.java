package com.bia.sandbox.mudlog;

import com.hashmapinc.tempus.WitsmlObjects.v1411.CsMudLogParameter;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

public interface ParameterDao {

    public void save(CsMudLogParameter csMudLogParameter, ObjMudLog objMudLog);

}
