package enlj.p106trading.mssqlv51.p10651basketball.pbperformbypunter.logics;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;

import enlj.p101admin.commonsv11.resource.logics.userpm.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.dpunterperformance.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.emonthperformance.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.fleagueperformance.logics.*;

public class PerformByPunterBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510410";
  
	/* Punter Type Constants */
    private final int kPT_SmartGroup	= 1;
    private final int kPT_NonSmart		= 2;
	private final int kPT_PTReport		= 3; 
	private final int kPT_SmartMaster	= 4;

	/* Task Ids */
	private final String kInitData				= "10651041001";	
	private final String kMainBetTypes			= "10651041004";
	private final String kLeagues				= "10651041005";
	private final String kGroups				= "10651041006";
	private final String kGroupPerformance		= "10651041007";
	private final String kAccountPerformance	= "10651041008";
	private final String kFilterData			= "10651041009";
	private final String kSmartMaster			= "10651041010";
	private final String kMasterList			= "10651041011";
	private final String kMasterPerformance		= "10651041012";

	private final String kA_DaysPerf_L12D	= "10651041021";
	private final String kB_MonthPerf       = "10651041031";
	private final String kC_LeaguePerf      = "10651041041";

	/* Server Row Names */
	private final String kSR_IsClientLogin		= "sr1";
    private final String kSR_MainBetTypes		= "sr2";
	private final String kSR_SmartMaster		= "sr3";
    private final String kSR_Leagues			= "sr4";
	private final String kSR_Groups				= "sr5";
	private final String kSR_BetTypes_Fltr		= "sr8";
	private final String kSR_Leagues_Fltr		= "sr9";
	private final String kSR_SpecialUserStatus	= "sr10";
    private final String kSR_CompanyUnits		= "sr11";
    private final String kSR_MasterList			= "sr12";

	/* Client Row Names */	
    private final String kCR_CommonList		= "cr1";
	private final String kCR_Performance	= "cr2";
	private final String kCR_SmartMaster	= "cr3";
	
	private final String kCR_A_DayPerf_L12D	= "cr11";	
	private final String kCR_B_MonthPerf    = "cr21";
	
	private final String kCR_C_LeaguePerf_L1M   = "cr31";
	private final String kCR_C_LeaguePerf_L3M	= "cr32";
    private final String kCR_C_LeaguePerf_L6M   = "cr33";
	private final String kCR_C_LeaguePerf_L1Y	= "cr34";
    private final String kCR_C_LeaguePerf_SEL   = "cr35";
    
	/* Status Ids */	
	private final String T1_FunctionPM			= "101";
	private final String T1_IsClientLogin		= "102";
	private final String T1_SpecialUserStatus	= "103";
	private final String T1_CompanyUnits		= "104";

	private final String T4_MainBetTypes	= "401";
	private final String T5_Leagues			= "501";
	private final String T6_Groups			= "601";	
	
	private final String T9_BetTypes	= "901";
	private final String T9_Leagues		= "902";

	private final String T10_SmartMaster 	= "1001";
	private final String T11_MasterList		= "1101";

	/* SmartMaster Data Fields */	
    private final int f_CompanyUnitId   = 0;

	/* Filter Data Fields */
	private final int f_SmartMasterId_CL	= 0;
	private final int f_FromDate_CL			= 1;
	private final int f_ToDate_CL			= 2;
	private final int f_IsSpecialUser		= 3;
    private final int f_CLCompanyUnitId		= 4;

   	public PerformByPunterBL ()
	{
		super ();		
	}
	
	/**
        A template method which has been extended from MSELogic.

        @see enlj.component.resource.logics.MSELogic#executeTask(Document oDocument, String oTaskId).
    */   
	public String executeTask (Document oDocument, String oTaskId)
	{		
		String oXMLString = "";
		setParams(oDocument);			

		if (oTaskId.equals (kInitData))
        {
            oXMLString = getInitData ();
        }
		else if (oTaskId.equals (kSmartMaster))
        {
            oXMLString = getSmartMaster ();
        }
		else if (oTaskId.equals (kMainBetTypes))
        {
            oXMLString = getMainBetTypes ();
        }
		else if (oTaskId.equals (kLeagues))
        {
            oXMLString = getLeagueList ();
        }		
		else if (oTaskId.equals (kGroups))
        {
            oXMLString = getGroupList ();
        }
		else if (oTaskId.equals (kMasterList))
        {
            oXMLString = getMasterList ();
        }
		else if (oTaskId.equals (kMasterPerformance))
        {
            oXMLString = getMasterPerformance ();
        }		
		else if (oTaskId.equals (kFilterData))
        {
            oXMLString = getFilterData ();
        }
		else if (oTaskId.equals (kGroupPerformance))
        {
            oXMLString = getGroupPerformance ();
        }
		else if (oTaskId.equals (kAccountPerformance))
        {
            oXMLString = getAccountPerformance ();
        }
		else if (oTaskId.equals (kA_DaysPerf_L12D))
        {
            oXMLString = getDaysPerformance_L12D ();
        }
		else if (oTaskId.equals (kB_MonthPerf))
        {
            oXMLString = getMonthPerformance_B ();
        }
        else if (oTaskId.equals (kC_LeaguePerf))
        {
            oXMLString = getLeaguePerformance ();
        }

		return oXMLString;
	}
	
	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));
		oBuffer.append (TradingUtil.getClientLoginStatus (this, T1_IsClientLogin, kSR_IsClientLogin));        
		oBuffer.append (BasketballUtil.checkSpecialUser_SPP (this, T1_SpecialUserStatus, kSR_SpecialUserStatus));        
        oBuffer.append (TradingUtil.getCompanyUnits (this, T1_CompanyUnits, kSR_CompanyUnits, ConstantsUtil.kFE_ChooseOne));

        return oBuffer.toString ();
    }

	private String getSmartMaster ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{
			String [] arrInfo = getParams (kCR_SmartMaster);			

			String oCondition = "";
			UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
			String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId]));

			if (oSmartMasterIds_PM.equals ("0") == false)
			{
				oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
			}

			String oSQL =
				" Select Distinct en_0651b04_smartmasterinfo.smartid, " +
					" en_0651b04_smartmasterinfo.mastercode, " +
					" 1 As Orderid " +
				" From en_0651b04_smartmasterinfo, en_0651b04_smartgroupinfo " +
				" Where en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " + 
					" en_0651b04_smartgroupinfo.mastergroupid Not In (" +
						" Select en_0651c91_ignoremasterids_spp.smartid From en_0651c91_ignoremasterids_spp " +
						" Where en_0651c91_ignoremasterids_spp.sportid = " + SportUtil.kS_BasketBall + ") And " +
					" en_0651b04_smartmasterinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
					" en_0651b04_smartmasterinfo.unitid = " + arrInfo [f_CompanyUnitId] +
					oCondition + 
				" Union All " +
				" Select 0 As smartid, " +
					" en_0651z00_firstelement.name_" + getLanguage () + " As mastercode, " +
					" 0 As Orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_All +
				" Order By orderid, mastercode ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_SmartMaster)); 
				oBuffer.append (getStatusXML (T10_SmartMaster, 1, "PerformByPunterBL:getSmartMaster:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T10_SmartMaster, -1, "PerformByPunterBL:getSmartMaster:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T10_SmartMaster, -1, "PerformByPunterBL:getSmartMaster:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }
	
	private String getMainBetTypes ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
			String [] arrInfo = getParams (kCR_CommonList);
			String oFromDate = arrInfo [f_FromDate_CL] + " 00:00:00";
			String oToDate = arrInfo [f_ToDate_CL] + " 23:59:59";

			String oSQL =
				" Select Distinct en_0251z00_bettype_main.mainbettypeid, " +
					" en_0251z00_bettype_main.mainbettype_en As mainbettype " +
				" From en_0651c02_betinfo_bbl, en_0251z00_bettype_main, " + 
					" en_0251z00_bettype, en_0251b02_accountinfo " +
				" Where en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
					" en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0251z00_bettype.sportid = " + SportUtil.kS_BasketBall + " And " +
	                " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
					" en_0251b02_accountinfo.unitid = " + arrInfo [f_CLCompanyUnitId] +
				" Order By mainbettypeid ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MainBetTypes)); 
				oBuffer.append (getStatusXML (T4_MainBetTypes, 1, "PerformByPunterBL:getMainBetTypes:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "PerformByPunterBL:getMainBetTypes:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "PerformByPunterBL:getMainBetTypes:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }

	private String getLeagueList ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
			String [] arrInfo = getParams (kCR_CommonList);
			String oFromDate = arrInfo [f_FromDate_CL] + " 00:00:00";
			String oToDate = arrInfo [f_ToDate_CL] + " 23:59:59";

			String oSQL =
				" Select Distinct en_0651c02_betinfo_bbl.leagueid, " +
					" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename " +
				" From en_0651c02_betinfo_bbl, en_0651b01_leagueinfo_bbl, en_0251b02_accountinfo " +
				" Where en_0651c02_betinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0651b01_leagueinfo_bbl.leagueid > 0 And " +
	                " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
					" en_0251b02_accountinfo.unitid = " + arrInfo [f_CLCompanyUnitId] +					
				" Order By leaguename ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Leagues)); 
				oBuffer.append (getStatusXML (T5_Leagues, 1, "PerformByPunterBL:getLeagueList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Leagues, -1, "PerformByPunterBL:getLeagueList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Leagues, -1, "PerformByPunterBL:getLeagueList:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }

	private String getGroupList ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{			
			String [] arrInfo = getParams (kCR_CommonList);
			String oFromDate = arrInfo [f_FromDate_CL] + " 00:00:00";
			String oToDate = arrInfo [f_ToDate_CL] + " 23:59:59";
			boolean bIsSpecialUser = arrInfo [f_IsSpecialUser].equals ("1");
			int nSmartMasterId = convertToInt (arrInfo [f_SmartMasterId_CL]);

			String oCondition = "";
			if (nSmartMasterId > 0)
				oCondition = " And en_0651b04_smartgroupinfo.mastergroupid = " + nSmartMasterId;

			UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
			String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CLCompanyUnitId]));

			if (oSmartGroupIds_PM.equals ("0") == false)
			{
				oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
			}

			if (bIsSpecialUser)
				oCondition += getSpecialUserCondition ();

			String oSQL =
				" Select Distinct en_0651b04_smartgroupinfo.smartid, " +
					" en_0651b04_smartgroupinfo.groupcode " +
				" From en_0651c02_betinfo_bbl, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts " +
				" Where en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
					" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
					" en_0651b04_smartgroupinfo.mastergroupid Not In (" +
						" Select en_0651c91_ignoremasterids_spp.smartid From en_0651c91_ignoremasterids_spp " +
						" Where en_0651c91_ignoremasterids_spp.sportid = " + SportUtil.kS_BasketBall + ") And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
					" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
					" en_0651b04_smartgroupinfo.isactive = 1 And " +
					" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CLCompanyUnitId] +
					oCondition +
				" Order By groupcode ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Groups)); 
				oBuffer.append (getStatusXML (T6_Groups, 1, "PerformByPunterBL:getGroupList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T6_Groups, -1, "PerformByPunterBL:getGroupList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T6_Groups, -1, "PerformByPunterBL:getGroupList:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }		
	
	private String getMasterList ()
	{
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{			
			String [] arrInfo = getParams (kCR_CommonList);
			String oFromDate = arrInfo [f_FromDate_CL] + " 00:00:00";
			String oToDate = arrInfo [f_ToDate_CL] + " 23:59:59";

			String oCondition = "";

			UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
			String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId]));

			if (oSmartMasterIds_PM.equals ("0") == false)
			{
				oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartmasterinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";				
			}

			String oSQL =
				" Select Distinct en_0651b04_smartmasterinfo.smartid, " +
					" en_0651b04_smartmasterinfo.mastercode " +
				" From en_0651c02_betinfo_bbl, en_0651b04_smartmasterinfo, en_0651b04_smartgroupinfo, " +
					" en_0651b04_smartgroupaccounts " +
				" Where en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
					" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
					" en_0651b04_smartgroupinfo.mastergroupid = en_0651b04_smartmasterinfo.smartid And " +
					" en_0651b04_smartmasterinfo.smartid Not In (" +
						" Select en_0651c91_ignoremasterids_spp.smartid From en_0651c91_ignoremasterids_spp " +
						" Where en_0651c91_ignoremasterids_spp.sportid = " + SportUtil.kS_BasketBall + ") And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
					" en_0651b04_smartmasterinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
					" en_0651b04_smartmasterinfo.unitid = " + arrInfo [f_CLCompanyUnitId] +
					oCondition +
				" Order By mastercode ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MasterList)); 
				oBuffer.append (getStatusXML (T11_MasterList, 1, "PerformByPunterBL:getMasterList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T11_MasterList, -1, "PerformByPunterBL:getMasterList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T11_MasterList, -1, "PerformByPunterBL:getMasterList:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();		
	}
	
	private String getFilterData ()
    {
        StringBuffer oBuffer = new StringBuffer ();        
		oBuffer.append (getBetTypes_Fltr ());        
        oBuffer.append (getLeagueList_Fltr ());

        return oBuffer.toString ();
    }

	private String getBetTypes_Fltr ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
			String [] arrInfo = getParams (kCR_CommonList);			
			String oFromDate = arrInfo [f_FromDate_CL] + " 00:00:00";
			String oToDate = arrInfo [f_ToDate_CL] + " 23:59:59";			

			String oSQL =
				" Select Distinct en_0251z00_bettype.bettypeid, " +
					" en_0251z00_bettype.bettype_" + getLanguage () + " As bettype, " +
					" 1 As orderid " +
				" From en_0651c02_betinfo_bbl, en_0251z00_bettype, en_0251b02_accountinfo " +
				" Where en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +					
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0251z00_bettype.sportid = " + SportUtil.kS_BasketBall + " And " +
	                " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
					" en_0251b02_accountinfo.unitid = " + arrInfo [f_CLCompanyUnitId] +
				" Union All " +
				" Select 0 As bettypeid, " +
					" en_0651z00_firstelement.name_" + getLanguage () + " As bettype, " +
					" 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_All +				
				" Order By orderid, bettypeid ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_BetTypes_Fltr)); 
				oBuffer.append (getStatusXML (T9_BetTypes, 1, "PerformByPunterBL:getBetTypes_Fltr:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T9_BetTypes, -1, "PerformByPunterBL:getBetTypes_Fltr:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T9_BetTypes, -1, "PerformByPunterBL:getBetTypes_Fltr:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }

	private String getLeagueList_Fltr ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
			String [] arrInfo = getParams (kCR_CommonList);
			String oFromDate = arrInfo [f_FromDate_CL] + " 00:00:00";
			String oToDate = arrInfo [f_ToDate_CL] + " 23:59:59";

			String oSQL =
				" Select Distinct en_0651c02_betinfo_bbl.leagueid, " +
					" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename, " +
					" 1 As orderid " +
				" From en_0651c02_betinfo_bbl, en_0651b01_leagueinfo_bbl, en_0251b02_accountinfo " +
				" Where en_0651c02_betinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0651b01_leagueinfo_bbl.leagueid > 0 And " +
		            " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
					" en_0251b02_accountinfo.unitid = " + arrInfo [f_CLCompanyUnitId] +
				" Union All " +
				" Select -1 As leagueid, " +
					" en_0651z00_firstelement.name_" + getLanguage () + " As leaguename, " +
					" 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_All +					
				" Order By leaguename ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Leagues_Fltr)); 
				oBuffer.append (getStatusXML (T9_Leagues, 1, "PerformByPunterBL:getLeagueList_Fltr:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T9_Leagues, -1, "PerformByPunterBL:getLeagueList_Fltr:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T9_Leagues, -1, "PerformByPunterBL:getLeagueList_Fltr:" + oException.toString ()));
            log (oBuffer.toString ());			
        }
        
        finally
        {
			try {oResultSet.close ();} catch (Exception oException) {oException.toString ();}
			oResultSet = null;

			try {oStatement.close ();} catch (Exception oException) {oException.toString ();}
			oStatement = null;
			
			oConnector.close ();
			oConnector = null;
        }

        return oBuffer.toString ();
    }

	private String getMasterPerformance ()
    {
        StringBuffer oBuffer = new StringBuffer ();
		String [] arrInfo = getParams (kCR_Performance);

		int nPunterTypeId = convertToInt (arrInfo [PPConstants.f_PunterTypeId]);

		if (nPunterTypeId == kPT_SmartMaster)
			oBuffer.append (getSmartMasterPerformance (arrInfo));

        return oBuffer.toString ();
    }
	
	private String getSmartMasterPerformance (String [] arrInfo)
	{
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getMasterPerformance_Smart (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}
	
	private String getGroupPerformance ()
    {
        StringBuffer oBuffer = new StringBuffer ();
		String [] arrInfo = getParams (kCR_Performance);

		int nPunterTypeId = convertToInt (arrInfo [PPConstants.f_PunterTypeId]);

		if (nPunterTypeId == kPT_SmartGroup)
			oBuffer.append (getSmartGroupPerformance (arrInfo));
		else if (nPunterTypeId == kPT_NonSmart)
			oBuffer.append (getNonSmartGroupPerformance (arrInfo));
		else if (nPunterTypeId == kPT_PTReport)
			oBuffer.append (getPTGroupPerformance (arrInfo));

        return oBuffer.toString ();
    }

	private String getSmartGroupPerformance (String [] arrInfo)
    {		
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getGroupPerformance_Smart (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getNonSmartGroupPerformance (String [] arrInfo)
    {		
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getGroupPerformance_NonSmart (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getPTGroupPerformance (String [] arrInfo)
    {		
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getGroupPerformance_PT (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getAccountPerformance ()
    {
        StringBuffer oBuffer = new StringBuffer ();
		String [] arrInfo = getParams (kCR_Performance);

		int nPunterTypeId = convertToInt (arrInfo [PPConstants.f_PunterTypeId]);

		if (nPunterTypeId == kPT_SmartGroup)
			oBuffer.append (getSmartAccountPerformance (arrInfo));
		else if (nPunterTypeId == kPT_NonSmart)
			oBuffer.append (getNonSmartAccountPerformance (arrInfo));
		else if (nPunterTypeId == kPT_PTReport)
			oBuffer.append (getPTAccountPerformance (arrInfo));

        return oBuffer.toString ();
    }

	private String getSmartAccountPerformance (String [] arrInfo)
    {		
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getAccountPerformance_Smart (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getNonSmartAccountPerformance (String [] arrInfo)
    {		
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getAccountPerformance_NonSmart (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getPTAccountPerformance (String [] arrInfo)
    {		
        PunterPerformanceObj oLogic = new PunterPerformanceObj (getDocument ());

        String oXMLString = oLogic.getAccountPerformance_PT (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

    private String getDaysPerformance_L12D ()
    {
        String [] arrInfo = getParams (kCR_A_DayPerf_L12D);
        MonthPerformanceObj oLogic = new MonthPerformanceObj (getDocument ());

        String oXMLString = oLogic.getDaysPerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
    }
    
    private String getMonthPerformance_B ()
    {
        String [] arrInfo = getParams (kCR_B_MonthPerf);
        MonthPerformanceObj oLogic = new MonthPerformanceObj (getDocument ());

        String oXMLString = oLogic.getMonthPerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
    }
    
    private String getLeaguePerformance ()
    {
		StringBuffer oBuffer = new StringBuffer ();
		
		oBuffer.append (getLeaguePerformance_L1M ());
		oBuffer.append (getLeaguePerformance_L3M ());
		oBuffer.append (getLeaguePerformance_L6M ());
		oBuffer.append (getLeaguePerformance_L1Y ());
        oBuffer.append (getLeaguePerformance_SEL ());

		return oBuffer.toString ();
	}

	private String getLeaguePerformance_L1M ()
    {		                   
		StringBuffer oBuffer = new StringBuffer ();
		String [] arrInfo = getParams (kCR_C_LeaguePerf_L1M);

        LeaguePerformanceObj oLogic = new LeaguePerformanceObj (getDocument ());

		String [] arrFilters_PP = arrInfo [LPConstants.f_FilterValues].split (DataUtil.kEXT_SEP);        
		int nPunterTypeId = convertToInt (arrFilters_PP [LPConstants.f_PunterTypeId_PL]);       

        if (nPunterTypeId == kPT_SmartGroup)
        	oBuffer.append (oLogic.getLeaguePerfPunter (SportUtil.kS_BasketBall, arrInfo));
        if (nPunterTypeId == kPT_SmartMaster)
        	oBuffer.append (oLogic.getLeaguePerfMaster (SportUtil.kS_BasketBall, arrInfo));

		return oBuffer.toString ();        
	}

	private String getLeaguePerformance_L3M ()
    {		        
		StringBuffer oBuffer = new StringBuffer ();           
		String [] arrInfo = getParams (kCR_C_LeaguePerf_L3M);

        LeaguePerformanceObj oLogic = new LeaguePerformanceObj (getDocument ());

		String [] arrFilters_PP = arrInfo [LPConstants.f_FilterValues].split (DataUtil.kEXT_SEP);        
		int nPunterTypeId = convertToInt (arrFilters_PP [LPConstants.f_PunterTypeId_PL]);       

        if (nPunterTypeId == kPT_SmartGroup)
        	oBuffer.append (oLogic.getLeaguePerfPunter (SportUtil.kS_BasketBall, arrInfo));
        if (nPunterTypeId == kPT_SmartMaster)
        	oBuffer.append (oLogic.getLeaguePerfMaster (SportUtil.kS_BasketBall, arrInfo));

		return oBuffer.toString ();
	}
	
	private String getLeaguePerformance_L6M ()
    {		                   
		StringBuffer oBuffer = new StringBuffer ();
		String [] arrInfo = getParams (kCR_C_LeaguePerf_L6M);

        LeaguePerformanceObj oLogic = new LeaguePerformanceObj (getDocument ());

		String [] arrFilters_PP = arrInfo [LPConstants.f_FilterValues].split (DataUtil.kEXT_SEP);        
		int nPunterTypeId = convertToInt (arrFilters_PP [LPConstants.f_PunterTypeId_PL]);       

        if (nPunterTypeId == kPT_SmartGroup)
        	oBuffer.append (oLogic.getLeaguePerfPunter (SportUtil.kS_BasketBall, arrInfo));
        if (nPunterTypeId == kPT_SmartMaster)
        	oBuffer.append (oLogic.getLeaguePerfMaster (SportUtil.kS_BasketBall, arrInfo));

		return oBuffer.toString ();        
	}

	private String getLeaguePerformance_L1Y ()
    {		        
		StringBuffer oBuffer = new StringBuffer ();           
		String [] arrInfo = getParams (kCR_C_LeaguePerf_L1Y);

        LeaguePerformanceObj oLogic = new LeaguePerformanceObj (getDocument ());

		String [] arrFilters_PP = arrInfo [LPConstants.f_FilterValues].split (DataUtil.kEXT_SEP);        
		int nPunterTypeId = convertToInt (arrFilters_PP [LPConstants.f_PunterTypeId_PL]);       

        if (nPunterTypeId == kPT_SmartGroup)
        	oBuffer.append (oLogic.getLeaguePerfPunter (SportUtil.kS_BasketBall, arrInfo));
        if (nPunterTypeId == kPT_SmartMaster)
        	oBuffer.append (oLogic.getLeaguePerfMaster (SportUtil.kS_BasketBall, arrInfo));

		return oBuffer.toString ();
	}   

	private String getLeaguePerformance_SEL ()
    {		        
		StringBuffer oBuffer = new StringBuffer ();           
		String [] arrInfo = getParams (kCR_C_LeaguePerf_SEL);

        LeaguePerformanceObj oLogic = new LeaguePerformanceObj (getDocument ());

		String [] arrFilters_PP = arrInfo [LPConstants.f_FilterValues].split (DataUtil.kEXT_SEP);        
		int nPunterTypeId = convertToInt (arrFilters_PP [LPConstants.f_PunterTypeId_PL]);       

        if (nPunterTypeId == kPT_SmartGroup)
        	oBuffer.append (oLogic.getLeaguePerfPunter (SportUtil.kS_BasketBall, arrInfo));
        if (nPunterTypeId == kPT_SmartMaster)
        	oBuffer.append (oLogic.getLeaguePerfMaster (SportUtil.kS_BasketBall, arrInfo));

		return oBuffer.toString ();
	}   

	protected String getSpecialUserCondition ()
	{
		String oCondition = " And en_0651b04_smartgroupinfo.smartid In (" +
				" Select en_0651b91_sppwinpunters_bbl.smartid " +
				" From en_0651b91_sppwinpunters_bbl " +
				" Where en_0651b91_sppwinpunters_bbl.typeid = 1 And " +	// WinPunters
					" en_0651b91_sppwinpunters_bbl.userid = " + getUserId () +
			") ";

		return oCondition;
	}

	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}