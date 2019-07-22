package com.bia.sandbox;

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
        if(csGeologyInterval.getTypeLithology() != null){
            data.set("typeLithology", csGeologyInterval.getTypeLithology().value());
        }
        if(csGeologyInterval.getMdTop() != null){
            data.set("mdTop", csGeologyInterval.getMdTop().getValue());
            data.set("mdTop_uom", csGeologyInterval.getMdTop().getUom().value());
        }
        if(csGeologyInterval.getMdBottom() != null){
            data.set("mdBottom", csGeologyInterval.getMdBottom().getValue());
            data.set("mdBottom_uom", csGeologyInterval.getMdBottom().getUom().value());
        }
        if(csGeologyInterval.getDTim() != null){
            data.set("dTim", BQUtil.convertToBQDate(csGeologyInterval.getDTim()));
        }
        if(csGeologyInterval.getTvdTop() != null){
            data.set("tvdTop", csGeologyInterval.getTvdTop().getValue());
            data.set("tvdTop_uom", csGeologyInterval.getTvdTop().getUom().value());
        }
        if(csGeologyInterval.getTvdBase() != null){
            data.set("tvdBase", csGeologyInterval.getTvdBase().getValue());
            data.set("tvdBase_uom", csGeologyInterval.getTvdBase().getUom().value());
        }
        if(csGeologyInterval.getRopAv() != null){
            data.set("ropAv", csGeologyInterval.getRopAv().getValue());
            data.set("ropAv_uom", csGeologyInterval.getRopAv().getUom().value());
        }
        if(csGeologyInterval.getRopMn() != null){
            data.set("ropMn", csGeologyInterval.getRopMn().getValue());
            data.set("ropMn_uom", csGeologyInterval.getRopMn().getUom().value());
        }
        if(csGeologyInterval.getRopMx() != null){
            data.set("ropMx", csGeologyInterval.getRopMx().getValue());
            data.set("ropMx_uom", csGeologyInterval.getRopMx().getUom().value());
        }
        if(csGeologyInterval.getWobAv() != null){
            data.set("wobAv", csGeologyInterval.getWobAv().getValue());
            data.set("wobAv_uom", csGeologyInterval.getWobAv().getUom().value());
        }
        if(csGeologyInterval.getTqAv() != null){
            data.set("tqAv", csGeologyInterval.getTqAv().getValue());
            data.set("tqAv_uom", csGeologyInterval.getTqAv().getUom().value());
        }
        if(csGeologyInterval.getRpmAv() != null){
            data.set("rpmAv", csGeologyInterval.getRpmAv().getValue());
            data.set("rpmAv_uom", csGeologyInterval.getRpmAv().getUom().value());
        }
        if(csGeologyInterval.getWtMudAv() != null){
            data.set("wtMudAv", csGeologyInterval.getWtMudAv().getValue());
            data.set("wtMudAv_uom", csGeologyInterval.getWtMudAv().getUom());
        }
        if(csGeologyInterval.getEcdTdAv() != null){
            data.set("ecdTdAv", csGeologyInterval.getEcdTdAv().getValue());
            data.set("ecdTdAv_uom", csGeologyInterval.getEcdTdAv().getUom());
        }
        if(csGeologyInterval.getDxcAv() != null){
            data.set("dxcAv", csGeologyInterval.getDxcAv());
        }
        if(csGeologyInterval.getShow() != null){
            if(csGeologyInterval.getShow().getShowRat() != null){
                data.set("show_showRat", csGeologyInterval.getShow().getShowRat().value());
            }
            if(csGeologyInterval.getShow().getStainColor() != null){
                data.set("show_stainColor", csGeologyInterval.getShow().getStainColor());
            }
            if(csGeologyInterval.getShow().getStainDistr() != null){
                data.set("show_stainDistr", csGeologyInterval.getShow().getStainDistr());
            }
            if(csGeologyInterval.getShow() != null){
                data.set("show_stainPc", csGeologyInterval.getShow().getStainPc().getValue());
                data.set("show_stainPc_uom", csGeologyInterval.getShow().getStainPc().getUom());
            }
            if(csGeologyInterval.getShow().getNatFlorColor() != null){
                data.set("show_natFlorColor", csGeologyInterval.getShow().getNatFlorColor());
            }
            if(csGeologyInterval.getShow() != null){
                data.set("show_natFlorPc", csGeologyInterval.getShow().getNatFlorPc().getValue());
                data.set("show_natFlorPc_uom", csGeologyInterval.getShow().getNatFlorPc().getUom());
            }
            if(csGeologyInterval.getShow().getNatFlorLevel() != null){
                data.set("show_natFlorLevel", csGeologyInterval.getShow().getNatFlorLevel().value());
            }
            if(csGeologyInterval.getShow().getNatFlorDesc() != null){
                data.set("show_natFlorDesc", csGeologyInterval.getShow().getNatFlorDesc());
            }
            if(csGeologyInterval.getShow().getCutColor() != null){
                data.set("show_cutColor", csGeologyInterval.getShow().getCutColor());
            }
            if(csGeologyInterval.getShow().getCutSpeed() != null){
                data.set("show_cutSpeed", csGeologyInterval.getShow().getCutSpeed().value());
            }
            if(csGeologyInterval.getShow().getCutStrength() != null){
                data.set("show_cutStrength", csGeologyInterval.getShow().getCutStrength());
            }
            if(csGeologyInterval.getShow().getCutForm() != null){
                data.set("show_cutForm", csGeologyInterval.getShow().getCutForm().value());
            }
            if(csGeologyInterval.getShow().getCutLevel() != null){
                data.set("show_cutLevel", csGeologyInterval.getShow().getCutLevel());
            }
            if(csGeologyInterval.getShow().getCutFlorColor() != null){
                data.set("show_cutFlorColor", csGeologyInterval.getShow().getCutFlorColor());
            }
            if(csGeologyInterval.getShow().getCutFlorSpeed() != null){
                data.set("show_cutFlorSpeed", csGeologyInterval.getShow().getCutFlorSpeed().value());
            }
            if(csGeologyInterval.getShow().getCutFlorStrength() != null){
                data.set("show_cutFlorStrength", csGeologyInterval.getShow().getCutFlorStrength());
            }
            if(csGeologyInterval.getShow().getCutFlorForm() != null){
                data.set("show_cutFlorForm", csGeologyInterval.getShow().getCutFlorForm().value());
            }
            if(csGeologyInterval.getShow().getCutFlorLevel() != null){
                data.set("show_cutFlorLevel", csGeologyInterval.getShow().getCutFlorLevel().value());
            }
            if(csGeologyInterval.getShow().getResidueColor() != null){
                data.set("show_residueColor", csGeologyInterval.getShow().getResidueColor());
            }
            if(csGeologyInterval.getShow().getShowDesc() != null){
                data.set("show_showDesc", csGeologyInterval.getShow().getShowDesc());
            }
            if(csGeologyInterval.getShow().getImpregnatedLitho() != null){
                data.set("show_impregnatedLitho", csGeologyInterval.getShow().getImpregnatedLitho());
            }
            if(csGeologyInterval.getShow().getOdor() != null){
                data.set("show_odor", csGeologyInterval.getShow().getOdor());
            }
        }
        if(csGeologyInterval.getChromatograph() != null) {
            if (csGeologyInterval.getChromatograph().getDTim() != null) {
                data.set("chromatograph_dTim", BQUtil.convertToBQDate(csGeologyInterval.getChromatograph().getDTim()));
            }
            if (csGeologyInterval.getChromatograph().getMdTop() != null) {
                data.set("chromatograph_mdTop", csGeologyInterval.getChromatograph().getMdTop().getValue());
                data.set("chromatograph_mdTop_uom", csGeologyInterval.getChromatograph().getMdTop().getUom().value());
            }
            if (csGeologyInterval.getChromatograph().getMdBottom() != null) {
                data.set("chromatograph_mdBottom", csGeologyInterval.getChromatograph().getMdBottom().getValue());
                data.set("chromatograph_mdBottom_uom", csGeologyInterval.getChromatograph().getMdBottom().getUom().value());
            }
            if (csGeologyInterval.getChromatograph().getWtMudIn() != null) {
                data.set("chromatograph_wtMudIn", csGeologyInterval.getChromatograph().getWtMudIn().getValue());
                data.set("chromatograph_wtMudIn_uom", csGeologyInterval.getChromatograph().getWtMudIn().getUom());
            }
            if (csGeologyInterval.getChromatograph().getWtMudOut() != null) {
                data.set("chromatograph_wtMudOut", csGeologyInterval.getChromatograph().getWtMudOut().getValue());
            }
            if (csGeologyInterval.getChromatograph().getChromType() != null) {
                data.set("chromatograph_chromType", csGeologyInterval.getChromatograph().getChromType());
            }
            if (csGeologyInterval.getChromatograph().getETimChromCycle() != null) {
                data.set("chromatograph_eTimChromCycle", csGeologyInterval.getChromatograph().getETimChromCycle().getValue());
                data.set("chromatograph_eTimChromCycle_uom", csGeologyInterval.getChromatograph().getETimChromCycle().getUom());
            }
            if (csGeologyInterval.getChromatograph().getChromIntRpt() != null) {
                data.set("chromatograph_chromIntRpt", BQUtil.convertToBQDate(csGeologyInterval.getChromatograph().getChromIntRpt()));
            }
            if (csGeologyInterval.getChromatograph().getMethAv() != null) {
                data.set("chromatograph_methAv", csGeologyInterval.getChromatograph().getMethAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getMethMn() != null) {
                data.set("chromatograph_methMn", csGeologyInterval.getChromatograph().getMethMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getMethMx() != null) {
                data.set("chromatograph_methMx", csGeologyInterval.getChromatograph().getMethMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getEthAv() != null) {
                data.set("chromatograph_ethAv", csGeologyInterval.getChromatograph().getEthAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getEthMn() != null) {
                data.set("chromatograph_ethMn", csGeologyInterval.getChromatograph().getEthMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getEthMx() != null) {
                data.set("chromatograph_ethMx", csGeologyInterval.getChromatograph().getEthMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getPropAv() != null) {
                data.set("chromatograph_propAv", csGeologyInterval.getChromatograph().getPropAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getPropMn() != null) {
                data.set("chromatograph_propMn", csGeologyInterval.getChromatograph().getPropMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getPropMx() != null) {
                data.set("chromatograph_propMx", csGeologyInterval.getChromatograph().getPropMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIbutAv() != null) {
                data.set("chromatograph_ibutAv", csGeologyInterval.getChromatograph().getIbutAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIbutMn() != null) {
                data.set("chromatograph_ibutMn", csGeologyInterval.getChromatograph().getIbutMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIbutMx() != null) {
                data.set("chromatograph_ibutMx", csGeologyInterval.getChromatograph().getIbutMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNbutAv() != null) {
                data.set("chromatograph_nbutAv", csGeologyInterval.getChromatograph().getNbutAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNbutMn() != null) {
                data.set("chromatograph_nbutMn", csGeologyInterval.getChromatograph().getNbutMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNbutMx() != null) {
                data.set("chromatograph_nbutMx", csGeologyInterval.getChromatograph().getNbutMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIpentAv() != null) {
                data.set("chromatograph_ipentAv", csGeologyInterval.getChromatograph().getIpentAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIpentMn() != null) {
                data.set("chromatograph_ipentMn", csGeologyInterval.getChromatograph().getIpentMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIpentMx() != null) {
                data.set("chromatograph_ipentMx", csGeologyInterval.getChromatograph().getIpentMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNpentAv() != null) {
                data.set("chromatograph_npentAv", csGeologyInterval.getChromatograph().getNpentAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNpentMn() != null) {
                data.set("chromatograph_npentMn", csGeologyInterval.getChromatograph().getNpentMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNpentMx() != null) {
                data.set("chromatograph_npentMx", csGeologyInterval.getChromatograph().getNpentMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getEpentAv() != null) {
                data.set("chromatograph_epentAv", csGeologyInterval.getChromatograph().getEpentAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getEpentMn() != null) {
                data.set("chromatograph_epentMn", csGeologyInterval.getChromatograph().getEpentMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getEpentMx() != null) {
                data.set("chromatograph_epentMx", csGeologyInterval.getChromatograph().getEpentMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIhexAv() != null) {
                data.set("chromatograph_ihexAv", csGeologyInterval.getChromatograph().getIhexAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIhexMn() != null) {
                data.set("chromatograph_ihexMn", csGeologyInterval.getChromatograph().getIhexMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getIhexMx() != null) {
                data.set("chromatograph_ihexMx", csGeologyInterval.getChromatograph().getIhexMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNhexAv() != null) {
                data.set("chromatograph_nhexAv", csGeologyInterval.getChromatograph().getNhexAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNhexMn() != null) {
                data.set("chromatograph_nhexMn", csGeologyInterval.getChromatograph().getNhexMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getNhexMx() != null) {
                data.set("chromatograph_nhexMx", csGeologyInterval.getChromatograph().getNhexMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getCo2Av() != null) {
                data.set("chromatograph_co2Av", csGeologyInterval.getChromatograph().getCo2Av().getValue());
            }
            if (csGeologyInterval.getChromatograph().getCo2Mn() != null) {
                data.set("chromatograph_co2Mn", csGeologyInterval.getChromatograph().getCo2Mn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getCo2Mx() != null) {
                data.set("chromatograph_co2Mx", csGeologyInterval.getChromatograph().getCo2Mx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getH2SAv() != null) {
                data.set("chromatograph_h2sAv", csGeologyInterval.getChromatograph().getH2SAv().getValue());
            }
            if (csGeologyInterval.getChromatograph().getH2SMn() != null) {
                data.set("chromatograph_h2sMn", csGeologyInterval.getChromatograph().getH2SMn().getValue());
            }
            if (csGeologyInterval.getChromatograph().getH2SMx() != null) {
                data.set("chromatograph_h2sMx", csGeologyInterval.getChromatograph().getH2SMx().getValue());
            }
            if (csGeologyInterval.getChromatograph().getAcetylene() != null) {
                data.set("chromatograph_acetylene", csGeologyInterval.getChromatograph().getAcetylene().getValue());
            }
        }
        if(csGeologyInterval.getMudGas() != null){
            if(csGeologyInterval.getMudGas().getGasAv() != null){
                data.set("mudGas_gasAv", csGeologyInterval.getMudGas().getGasAv().getValue());
                data.set("mudGas_gasAv_uom", csGeologyInterval.getMudGas().getGasAv().getUom());
            }
            if(csGeologyInterval.getMudGas().getGasPeak() != null){
                data.set("mudGas_gasPeak", csGeologyInterval.getMudGas().getGasPeak().getValue());
                data.set("mudGas_gasPeak_uom", csGeologyInterval.getMudGas().getGasPeak().getUom());
            }
            if(csGeologyInterval.getMudGas().getGasPeakType() != null){
                data.set("mudGas_gasPeakType", csGeologyInterval.getMudGas().getGasPeakType().value());
            }
            if(csGeologyInterval.getMudGas().getGasBackgnd() != null){
                data.set("mudGas_gasBackgnd", csGeologyInterval.getMudGas().getGasBackgnd().getValue());
                data.set("mudGas_gasBackgnd_uom", csGeologyInterval.getMudGas().getGasBackgnd().getUom());
            }
            if(csGeologyInterval.getMudGas().getGasConAv() != null){
                data.set("mudGas_gasConAv", csGeologyInterval.getMudGas().getGasConAv().getValue());
                data.set("mudGas_gasConAv_uom", csGeologyInterval.getMudGas().getGasConAv().getUom());
            }
            if(csGeologyInterval.getMudGas().getGasConMx() != null){
                data.set("mudGas_gasConMx", csGeologyInterval.getMudGas().getGasConMx().getValue());
                data.set("mudGas_gasConMx_uom", csGeologyInterval.getMudGas().getGasConMx().getUom());
            }
            if(csGeologyInterval.getMudGas().getGasTrip() != null){
                data.set("mudGas_gasTrip", csGeologyInterval.getMudGas().getGasTrip().getValue());
                data.set("mudGas_gasTrip_uom", csGeologyInterval.getMudGas().getGasTrip().getUom());
            }
        }
        if(csGeologyInterval.getDensBulk() != null){
            data.set("densBulk", csGeologyInterval.getDensBulk().getValue());
            data.set("densBulk_uom", csGeologyInterval.getDensBulk().getUom());
        }
        if(csGeologyInterval.getDensShale() != null){
            data.set("densShale", csGeologyInterval.getDensShale().getValue());
            data.set("densShale_uom", csGeologyInterval.getDensShale().getUom());
        }
        if(csGeologyInterval.getCalcite() != null){
            data.set("calcite", csGeologyInterval.getCalcite().getValue());
            data.set("calcite_uom", csGeologyInterval.getCalcite().getUom());
        }
        if(csGeologyInterval.getDolomite() != null){
            data.set("dolomite", csGeologyInterval.getDolomite().getValue());
            data.set("dolomite_uom", csGeologyInterval.getDolomite().getUom());
        }
        if(csGeologyInterval.getCec() != null){
            data.set("cec", csGeologyInterval.getCec().getValue());
            data.set("cec_uom", csGeologyInterval.getCec().getUom().value());
        }
        if(csGeologyInterval.getCalcStab() != null){
            data.set("calcStab", csGeologyInterval.getCalcStab().getValue());
            data.set("calcStab_uom", csGeologyInterval.getCalcStab().getUom());
        }
        if(csGeologyInterval.getSizeMn() != null){
            data.set("sizeMn", csGeologyInterval.getSizeMn().getValue());
            data.set("sizeMn_uom", csGeologyInterval.getSizeMn().getUom());
        }
        if(csGeologyInterval.getSizeMx() != null){
            data.set("sizeMx", csGeologyInterval.getSizeMx().getValue());
            data.set("sizeMx_uom", csGeologyInterval.getSizeMx().getUom());
        }
        if(csGeologyInterval.getLenPlug() != null){
            data.set("lenPlug", csGeologyInterval.getLenPlug().getValue());
            data.set("lenPlug_uom", csGeologyInterval.getLenPlug().getUom());
        }
        if(csGeologyInterval.getDescription() != null){
            data.set("description", csGeologyInterval.getDescription());
        }
        if(csGeologyInterval.getCuttingFluid() != null){
            data.set("cuttingFluid", csGeologyInterval.getCuttingFluid());
        }
        if(csGeologyInterval.getCleaningMethod() != null){
            data.set("cleaningMethod", csGeologyInterval.getCleaningMethod());
        }
        if(csGeologyInterval.getDryingMethod() != null){
            data.set("dryingMethod", csGeologyInterval.getDryingMethod());
        }
        if(csGeologyInterval.getCommonTime() != null){
            if(csGeologyInterval.getCommonTime().getDTimCreation() != null){
                data.set("commonTime_dTimCreation", BQUtil.convertToBQDate(csGeologyInterval.getCommonTime().getDTimCreation()));
            }
            if(csGeologyInterval.getCommonTime().getDTimLastChange() != null){
                data.set("commonTime_dTimLastChange", BQUtil.convertToBQDate(csGeologyInterval.getCommonTime().getDTimLastChange()));
            }
        }

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
