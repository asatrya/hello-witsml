package com.bia.sandbox;

import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.hashmapinc.tempus.WitsmlObjects.v1411.CsGeologyInterval;
import com.hashmapinc.tempus.WitsmlObjects.v1411.ObjMudLog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MudLogGeologyIntervalDaoImpl implements MudLogGeologyIntervalDao {

    private BigQuery bigQuery;
    private TableId tableId;
    private final static String TABLE_NAME = "mudLogs_geologyInterval";

    public MudLogGeologyIntervalDaoImpl() throws IOException {
        bigQuery = ConnectionFactory.getConnection();
        tableId = TableId.of(GlobalOptions.getInstance().getDataSetName(), MudLogGeologyIntervalDaoImpl.TABLE_NAME);
    }

    @Override
    public void save(CsGeologyInterval csGeologyInterval, ObjMudLog objMudLog) {
        TableRow data = new TableRow();
        data.set("uid", csGeologyInterval.getUid());
        data.set("uid_mudLogs", objMudLog.getUid());
        data.set("typeLithology", csGeologyInterval.getTypeLithology().value());
        data.set("mdTop", csGeologyInterval.getMdTop().getValue());
        data.set("mdTop_uom", csGeologyInterval.getMdTop().getUom().value());
        data.set("mdBottom", csGeologyInterval.getMdBottom().getValue());
        data.set("mdBottom_uom", csGeologyInterval.getMdBottom().getUom().value());
        data.set("dTim", BQUtil.convertToBQDate(csGeologyInterval.getDTim()));
        data.set("tvdTop", csGeologyInterval.getTvdTop().getValue());
        data.set("tvdTop_uom", csGeologyInterval.getTvdTop().getUom().value());
        data.set("tvdBase", csGeologyInterval.getTvdBase().getValue());
        data.set("tvdBase_uom", csGeologyInterval.getTvdBase().getUom().value());
        data.set("ropAv", csGeologyInterval.getRopAv().getValue());
        data.set("ropAv_uom", csGeologyInterval.getRopAv().getUom().value());
        data.set("ropMn", csGeologyInterval.getRopMn().getValue());
        data.set("ropMn_uom", csGeologyInterval.getRopMn().getUom().value());
        data.set("ropMx", csGeologyInterval.getRopMx().getValue());
        data.set("ropMx_uom", csGeologyInterval.getRopMx().getUom().value());
        data.set("wobAv", csGeologyInterval.getWobAv().getValue());
        data.set("wobAv_uom", csGeologyInterval.getWobAv().getUom().value());
        data.set("tqAv", csGeologyInterval.getTqAv().getValue());
        data.set("tqAv_uom", csGeologyInterval.getTqAv().getUom().value());
        data.set("rpmAv", csGeologyInterval.getRpmAv().getValue());
        data.set("rpmAv_uom", csGeologyInterval.getRpmAv().getUom().value());
        data.set("wtMudAv", csGeologyInterval.getWtMudAv().getValue());
        data.set("wtMudAv_uom", csGeologyInterval.getWtMudAv().getUom());
        data.set("ecdTdAv", csGeologyInterval.getEcdTdAv().getValue());
        data.set("ecdTdAv_uom", csGeologyInterval.getEcdTdAv().getUom());
        data.set("dxcAv", csGeologyInterval.getDxcAv());
        data.set("show_showRat", csGeologyInterval.getShow().getShowRat().value());
        data.set("show_stainColor", csGeologyInterval.getShow().getStainColor());
        data.set("show_stainDistr", csGeologyInterval.getShow().getStainDistr());
        data.set("show_stainPc", csGeologyInterval.getShow().getStainPc().getValue());
        data.set("show_stainPc_uom", csGeologyInterval.getShow().getStainPc().getUom());
        data.set("show_natFlorColor", csGeologyInterval.getShow().getNatFlorColor());
        data.set("show_natFlorPc", csGeologyInterval.getShow().getNatFlorPc().getValue());
        data.set("show_natFlorPc_uom", csGeologyInterval.getShow().getNatFlorPc().getUom());
        data.set("show_natFlorLevel", csGeologyInterval.getShow().getNatFlorLevel().value());
        data.set("show_natFlorDesc", csGeologyInterval.getShow().getNatFlorDesc());
        data.set("show_cutColor", csGeologyInterval.getShow().getCutColor());
        data.set("show_cutSpeed", csGeologyInterval.getShow().getCutSpeed().value());
        data.set("show_cutStrength", csGeologyInterval.getShow().getCutStrength());
        data.set("show_cutForm", csGeologyInterval.getShow().getCutForm().value());
        data.set("show_cutLevel", csGeologyInterval.getShow().getCutLevel());
        data.set("show_cutFlorColor", csGeologyInterval.getShow().getCutFlorColor());
        data.set("show_cutFlorSpeed", csGeologyInterval.getShow().getCutFlorSpeed().value());
        data.set("show_cutFlorStrength", csGeologyInterval.getShow().getCutFlorStrength());
        data.set("show_cutFlorForm", csGeologyInterval.getShow().getCutFlorForm().value());
        data.set("show_cutFlorLevel", csGeologyInterval.getShow().getCutFlorLevel().value());
        data.set("show_residueColor", csGeologyInterval.getShow().getResidueColor());
        data.set("show_showDesc", csGeologyInterval.getShow().getShowDesc());
        data.set("show_impregnatedLitho", csGeologyInterval.getShow().getImpregnatedLitho());
        data.set("show_odor", csGeologyInterval.getShow().getOdor());
        data.set("chromatograph_dTim", BQUtil.convertToBQDate(csGeologyInterval.getChromatograph().getDTim()));
        data.set("chromatograph_mdTop", csGeologyInterval.getChromatograph().getMdTop().getValue());
        data.set("chromatograph_mdTop_uom", csGeologyInterval.getChromatograph().getMdTop().getUom().value());
        data.set("chromatograph_mdBottom", csGeologyInterval.getChromatograph().getMdBottom().getValue());
        data.set("chromatograph_mdBottom_uom", csGeologyInterval.getChromatograph().getMdBottom().getUom().value());
        data.set("chromatograph_wtMudIn", csGeologyInterval.getChromatograph().getWtMudIn().getValue());
        data.set("chromatograph_wtMudIn_uom", csGeologyInterval.getChromatograph().getWtMudIn().getUom());
        data.set("chromatograph_wtMudOut", csGeologyInterval.getChromatograph().getWtMudOut().getValue());
        data.set("chromatograph_chromType", csGeologyInterval.getChromatograph().getChromType());
        data.set("chromatograph_eTimChromCycle", csGeologyInterval.getChromatograph().getETimChromCycle().getValue());
        data.set("chromatograph_eTimChromCycle_uom", csGeologyInterval.getChromatograph().getETimChromCycle().getUom());
        data.set("chromatograph_chromIntRpt", BQUtil.convertToBQDate(csGeologyInterval.getChromatograph().getChromIntRpt()));
        data.set("chromatograph_methAv", csGeologyInterval.getChromatograph().getMethAv().getValue());
        data.set("chromatograph_methMn", csGeologyInterval.getChromatograph().getMethMn().getValue());
        data.set("chromatograph_methMx", csGeologyInterval.getChromatograph().getMethMx().getValue());
        data.set("chromatograph_ethAv", csGeologyInterval.getChromatograph().getEthAv().getValue());
        data.set("chromatograph_ethMn", csGeologyInterval.getChromatograph().getEthMn().getValue());
        data.set("chromatograph_ethMx", csGeologyInterval.getChromatograph().getEthMx().getValue());
        data.set("chromatograph_propAv", csGeologyInterval.getChromatograph().getPropAv().getValue());
        data.set("chromatograph_propMn", csGeologyInterval.getChromatograph().getPropMn().getValue());
        data.set("chromatograph_propMx", csGeologyInterval.getChromatograph().getPropMx().getValue());
        data.set("chromatograph_ibutAv", csGeologyInterval.getChromatograph().getIbutAv().getValue());
        data.set("chromatograph_ibutMn", csGeologyInterval.getChromatograph().getIbutMn().getValue());
        data.set("chromatograph_ibutMx", csGeologyInterval.getChromatograph().getIbutMx().getValue());
        data.set("chromatograph_nbutAv", csGeologyInterval.getChromatograph().getNbutAv().getValue());
        data.set("chromatograph_nbutMn", csGeologyInterval.getChromatograph().getNbutMn().getValue());
        data.set("chromatograph_nbutMx", csGeologyInterval.getChromatograph().getNbutMx().getValue());
        data.set("chromatograph_ipentAv", csGeologyInterval.getChromatograph().getIpentAv().getValue());
        data.set("chromatograph_ipentMn", csGeologyInterval.getChromatograph().getIpentMn().getValue());
        data.set("chromatograph_ipentMx", csGeologyInterval.getChromatograph().getIpentMx().getValue());
        data.set("chromatograph_npentAv", csGeologyInterval.getChromatograph().getNpentAv().getValue());
        data.set("chromatograph_npentMn", csGeologyInterval.getChromatograph().getNpentMn().getValue());
        data.set("chromatograph_npentMx", csGeologyInterval.getChromatograph().getNpentMx().getValue());
        data.set("chromatograph_epentAv", csGeologyInterval.getChromatograph().getEpentAv().getValue());
        data.set("chromatograph_epentMn", csGeologyInterval.getChromatograph().getEpentMn().getValue());
        data.set("chromatograph_epentMx", csGeologyInterval.getChromatograph().getEpentMx().getValue());
        data.set("chromatograph_ihexAv", csGeologyInterval.getChromatograph().getIhexAv().getValue());
        data.set("chromatograph_ihexMn", csGeologyInterval.getChromatograph().getIhexMn().getValue());
        data.set("chromatograph_ihexMx", csGeologyInterval.getChromatograph().getIhexMx().getValue());
        data.set("chromatograph_nhexAv", csGeologyInterval.getChromatograph().getNhexAv().getValue());
        data.set("chromatograph_nhexMn", csGeologyInterval.getChromatograph().getNhexMn().getValue());
        data.set("chromatograph_nhexMx", csGeologyInterval.getChromatograph().getNhexMx().getValue());
        data.set("chromatograph_co2Av", csGeologyInterval.getChromatograph().getCo2Av().getValue());
        data.set("chromatograph_co2Mn", csGeologyInterval.getChromatograph().getCo2Mn().getValue());
        data.set("chromatograph_co2Mx", csGeologyInterval.getChromatograph().getCo2Mx().getValue());
        data.set("chromatograph_h2sAv", csGeologyInterval.getChromatograph().getH2SAv().getValue());
        data.set("chromatograph_h2sMn", csGeologyInterval.getChromatograph().getH2SMn().getValue());
        data.set("chromatograph_h2sMx", csGeologyInterval.getChromatograph().getH2SMx().getValue());
        data.set("chromatograph_acetylene", csGeologyInterval.getChromatograph().getAcetylene().getValue());
        data.set("mudGas_gasAv", csGeologyInterval.getMudGas().getGasAv().getValue());
        data.set("mudGas_gasAv_uom", csGeologyInterval.getMudGas().getGasAv().getUom());
        data.set("mudGas_gasPeak", csGeologyInterval.getMudGas().getGasPeak().getValue());
        data.set("mudGas_gasPeak_uom", csGeologyInterval.getMudGas().getGasPeak().getUom());
        data.set("mudGas_gasPeakType", csGeologyInterval.getMudGas().getGasPeakType().value());
        data.set("mudGas_gasBackgnd", csGeologyInterval.getMudGas().getGasBackgnd().getValue());
        data.set("mudGas_gasBackgnd_uom", csGeologyInterval.getMudGas().getGasBackgnd().getUom());
        data.set("mudGas_gasConAv", csGeologyInterval.getMudGas().getGasConAv().getValue());
        data.set("mudGas_gasConAv_uom", csGeologyInterval.getMudGas().getGasConAv().getUom());
        data.set("mudGas_gasConMx", csGeologyInterval.getMudGas().getGasConMx().getValue());
        data.set("mudGas_gasConMx_uom", csGeologyInterval.getMudGas().getGasConMx().getUom());
        data.set("mudGas_gasTrip", csGeologyInterval.getMudGas().getGasTrip().getValue());
        data.set("mudGas_gasTrip_uom", csGeologyInterval.getMudGas().getGasTrip().getUom());
        data.set("densBulk", csGeologyInterval.getDensBulk().getValue());
        data.set("densBulk_uom", csGeologyInterval.getDensBulk().getUom());
        data.set("densShale", csGeologyInterval.getDensShale().getValue());
        data.set("densShale_uom", csGeologyInterval.getDensShale().getUom());
        data.set("calcite", csGeologyInterval.getCalcite().getValue());
        data.set("calcite_uom", csGeologyInterval.getCalcite().getUom());
        data.set("dolomite", csGeologyInterval.getDolomite().getValue());
        data.set("dolomite_uom", csGeologyInterval.getDolomite().getUom());
        data.set("cec", csGeologyInterval.getCec().getValue());
        data.set("cec_uom", csGeologyInterval.getCec().getUom().value());
        data.set("calcStab", csGeologyInterval.getCalcStab().getValue());
        data.set("calcStab_uom", csGeologyInterval.getCalcStab().getUom());
        data.set("sizeMn", csGeologyInterval.getSizeMn().getValue());
        data.set("sizeMn_uom", csGeologyInterval.getSizeMn().getUom());
        data.set("sizeMx", csGeologyInterval.getSizeMx().getValue());
        data.set("sizeMx_uom", csGeologyInterval.getSizeMx().getUom());
        data.set("lenPlug", csGeologyInterval.getLenPlug().getValue());
        data.set("lenPlug_uom", csGeologyInterval.getLenPlug().getUom());
        data.set("description", csGeologyInterval.getDescription());
        data.set("cuttingFluid", csGeologyInterval.getCuttingFluid());
        data.set("cleaningMethod", csGeologyInterval.getCleaningMethod());
        data.set("dryingMethod", csGeologyInterval.getDryingMethod());
        data.set("commonTime_dTimCreation", BQUtil.convertToBQDate(csGeologyInterval.getCommonTime().getDTimCreation()));
        data.set("commonTime_dTimLastChange", BQUtil.convertToBQDate(csGeologyInterval.getCommonTime().getDTimLastChange()));

        InsertAllRequest request = null;
        try{
            request = InsertAllRequest.newBuilder(this.tableId).addRow(data).build();
            InsertAllResponse response = this.bigQuery.insertAll(request);

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    System.out.println(entry.toString());
                }
            }
        }catch (IllegalArgumentException ex){
            System.out.println(request);
            ex.printStackTrace();
        }
    }
}
