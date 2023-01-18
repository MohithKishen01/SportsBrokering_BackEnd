package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbygroup.logics;

import java.sql.*;
import org.w3c.dom.*;
import java.math.BigInteger;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;

import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbygroup.logics.livebets.*;
import enlj.p106trading.mssqlv51.p10651basketball.rbbetsbygroup.logics.nonlivebets.*;

import enlj.p101admin.commonsv11.resource.logics.userpm.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.cwagerlist.abetlist.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.emonthperformance.logics.*;

public class BetsByGroupBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510407";    	

	BigInteger kLiveBetsL50TaskIds_Start = null;
	BigInteger kLiveBetsL50TaskIds_End = null;

	BigInteger kNonLiveBetsL50TaskIds_Start = null;
	BigInteger kNonLiveBetsL50TaskIds_End = null;
    
    /* Report Type Constants */
	private final int kRT_PendingBets	= 7;
	private final int kRT_SettledBets	= 8;
    
	/* Task Ids */
	private final String kInitData	    = "10651040701";
	private final String kMainBetTypes  = "10651040704";
 	private final String kLeagues       = "10651040705";
 	private final String kSmartMasters  = "10651040706";
 	private final String kSmartGroups   = "10651040707";
 	private final String kBetList       = "10651040708";

	private final String kA_PunterPerformance   = "10651040721";
	private final String kB_DaysPerformance     = "10651040731";

	/* Server Row Names */
	private final String kSR_IsClientLogin		= "sr1";
	private final String kSR_MainBetTypes		= "sr2";
	private final String kSR_Leagues			= "sr3";
	private final String kSR_SmartMasters		= "sr4";
	private final String kSR_SmartGroups		= "sr5";
	private final String kSR_BetList			= "sr6";	// Is used from the BetListObj caller
	private final String kSR_SpecialUserStatus	= "sr7";
	private final String kSR_CompanyUnits		= "sr8";
	private final String kSR_BLTypePM			= "sr9";

	/* Client Row Names */
    private final String kCR_CommonData		= "cr1";
    private final String kCR_BetList		= "cr2";
    private final String kCR_MoreFilters	= "cr3";

	private final String kCR_A_PunterPerformance= "cr21";
	private final String kCR_B_DayPerformance	= "cr31";

	/* Status Ids */		
	private final String GT_BetList	= "51";
	
	private final String T1_FunctionPM			= "101";
	private final String T1_IsClientLogin		= "102";
	private final String T1_SpecialUserStatus	= "103";
	private final String T1_CompanyUnits		= "104";
	
	private final String T4_MainBetTypes	= "401";
	private final String T5_Leagues	        = "501";
	private final String T6_SmartMasters    = "601";
	private final String T7_SmartGroups	    = "701";
	private final String T8_BLTypePM		= "802";

	/* Filter Data Fields */	
	private final int f_FDCompanyUnitId	= 0; // Not in Filter
	private final int f_FDReportTypeId	= 1;
	private final int f_FDFromDate	    = 2;
	private final int f_FDToDate	    = 3;
	private final int f_FDIsSpecialUser	= 4;

   	public BetsByGroupBL ()
	{
		super ();	
	    kLiveBetsL50TaskIds_Start = new BigInteger ("10651040774");
	    kLiveBetsL50TaskIds_End = new BigInteger ("10651040776");	

	    kNonLiveBetsL50TaskIds_Start = new BigInteger ("10651040777");
	    kNonLiveBetsL50TaskIds_End = new BigInteger ("10651040779");	
	}
	
	/**
        A template method which has been extended from MSELogic.

        @see enlj.component.resource.logics.MSELogic#executeTask(Document oDocument, String oTaskId).
    */   
	public String executeTask (Document oDocument, String oTaskId)
	{		
	    BigInteger biTaskId = new BigInteger (oTaskId);

		String oXMLString = "";
		setParams(oDocument);			

		if (oTaskId.equals (kInitData))
        {
            oXMLString = getInitData ();
        }
        else if (oTaskId.equals (kMainBetTypes))
        {
            oXMLString = getMainBetTypeList ();
        }
		else if (oTaskId.equals (kLeagues))
        {
            oXMLString = getLeagueList ();
        }
        else if (oTaskId.equals (kSmartMasters))
        {
            oXMLString = getSmartMasterList ();
        }
        else if (oTaskId.equals (kSmartGroups))
        {
            oXMLString = getSmartGroupList ();
        }
        else if (oTaskId.equals (kBetList))
        {
            oXMLString = getBetList ();
        }
        else if (oTaskId.equals (kA_PunterPerformance))
        {
            oXMLString = getPunterPerformance_A ();
        }
        else if (oTaskId.equals (kB_DaysPerformance))
        {
            oXMLString = getDaysPerformance_B ();
        }
		else if ((biTaskId.compareTo (kLiveBetsL50TaskIds_Start) == 1 || biTaskId.compareTo (kLiveBetsL50TaskIds_Start) == 0) && 
			(biTaskId.compareTo (kLiveBetsL50TaskIds_End) == -1 || biTaskId.compareTo (kLiveBetsL50TaskIds_End) == 0))
		{
			LiveBetsBL oLiveBetsBL = new LiveBetsBL ();
			oXMLString = oLiveBetsBL.executeTask (oDocument, oTaskId);
		}
		else if ((biTaskId.compareTo (kNonLiveBetsL50TaskIds_Start) == 1 || biTaskId.compareTo (kNonLiveBetsL50TaskIds_Start) == 0) && 
			(biTaskId.compareTo (kNonLiveBetsL50TaskIds_End) == -1 || biTaskId.compareTo (kNonLiveBetsL50TaskIds_End) == 0))
		{
			NonLiveBetsBL oNonLiveBetsBL = new NonLiveBetsBL ();
			oXMLString = oNonLiveBetsBL.executeTask (oDocument, oTaskId);
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

    private String getMainBetTypeList ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
            String [] arrInfo = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (arrInfo [f_FDReportTypeId]);

		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getMainBetTypesSQL_PB (arrInfo);
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getMainBetTypesSQL_SB (arrInfo);

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MainBetTypes)); 
				oBuffer.append (getStatusXML (T4_MainBetTypes, 1, "BetsByGroupBL:getMainBetTypeList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "BetsByGroupBL:getMainBetTypeList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "BetsByGroupBL:getMainBetTypeList:" + oException.toString ()));
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
    
    private String getMainBetTypesSQL_PB (String [] arrInfo)
    {
		String oSQL =
			" Select Distinct en_0251z00_bettype_main.mainbettypeid, " +
				" en_0251z00_bettype_main.mainbettype_" + getLanguage () + " As mainbettype " +
			" From en_0251z00_bettype_main, en_0251z00_bettype, en_0651c02_betstatus_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
			    " en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " + 
			    " en_0651c02_betstatus_bbl.accountid = en_0251b02_accountinfo.accountid And " +
			    " en_0251b02_accountinfo.unitid = " + arrInfo [f_FDCompanyUnitId] +
				getDateCondition_PB (arrInfo) + 
			" Order By mainbettype ";

        return oSQL;
    }
    
    private String getMainBetTypesSQL_SB (String [] arrInfo)
    {
        String oFromDate = arrInfo [f_FDFromDate] + " 00:00:00";
        String oToDate = arrInfo [f_FDToDate] + " 23:59:59";        

		String oSQL =
			" Select Distinct en_0251z00_bettype_main.mainbettypeid, " +
				" en_0251z00_bettype_main.mainbettype_" + getLanguage () + " As mainbettype " +
			" From en_0251z00_bettype_main, en_0251z00_bettype, en_0651c02_betinfo_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
			    " en_0251z00_bettype.bettypeid = en_0651c02_betinfo_bbl.bettypeid And " + 
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
                " en_0651c02_betinfo_bbl.settlementby > 0 And " + 
			    " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
			    " en_0251b02_accountinfo.unitid = " + arrInfo [f_FDCompanyUnitId] +
			" Order By mainbettype ";

        return oSQL;
    }  
    
	private String getLeagueList ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
            String [] arrInfo = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (arrInfo [f_FDReportTypeId]);
		    
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getLeagueListSQL_PB (arrInfo);
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getLeagueListSQL_SB (arrInfo);

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Leagues)); 
				oBuffer.append (getStatusXML (T5_Leagues, 1, "BetsByGroupBL:getLeagueList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Leagues, -1, "BetsByGroupBL:getLeagueList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Leagues, -1, "BetsByGroupBL:getLeagueList:" + oException.toString ()));
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
    
    private String getLeagueListSQL_PB (String [] arrInfo)
    {
		String oSQL =
			" Select Distinct en_0651c02_betstatus_bbl.leagueid, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename " +
			" From en_0651c02_betstatus_bbl, en_0651b01_leagueinfo_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0651c02_betstatus_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
				" en_0651b01_leagueinfo_bbl.leagueid > 0 And " + 
			    " en_0651c02_betstatus_bbl.accountid = en_0251b02_accountinfo.accountid And " +
			    " en_0251b02_accountinfo.unitid = " + arrInfo [f_FDCompanyUnitId] +
				getDateCondition_PB (arrInfo) + 
			" Order By leaguename ";
		
        return oSQL;
    }
    
    private String getLeagueListSQL_SB (String [] arrInfo)
    {
        String oFromDate = arrInfo [f_FDFromDate] + " 00:00:00";
        String oToDate = arrInfo [f_FDToDate] + " 23:59:59";        

		String oSQL =
			" Select Distinct en_0651c02_betinfo_bbl.leagueid, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename " +
			" From en_0651c02_betinfo_bbl, en_0651b01_leagueinfo_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0651c02_betinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
				" en_0651b01_leagueinfo_bbl.leagueid > 0 And " +
                " en_0651c02_betinfo_bbl.settlementby > 0 And " + 
			    " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
			    " en_0251b02_accountinfo.unitid = " + arrInfo [f_FDCompanyUnitId] +
			" Order By leaguename ";
		
        return oSQL;
    }    

    private String getSmartMasterList ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {
		    String [] arrInfo = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (arrInfo [f_FDReportTypeId]);
		
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getSmartMasterListSQL_PB (arrInfo);
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getSmartMasterListSQL_SB (arrInfo);		               

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_SmartMasters));
                oBuffer.append (getStatusXML (T6_SmartMasters, 1, "BetsByGroupBL:getSmartMasterList:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T6_SmartMasters, -1, "BetsByGroupBL:getSmartMasterList:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T6_SmartMasters, -1, "BetsByGroupBL:getSmartMasterList" + oException.toString ()));
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
    
    private String getSmartMasterListSQL_PB (String [] arrInfo)
    {
		String oCondition = "";
		boolean bIsSpecialUser = arrInfo [f_FDIsSpecialUser].equals ("1");

		UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
		String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_FDCompanyUnitId]));

		if (oSmartMasterIds_PM.equals ("0") == false)
		{
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
		}

		if (bIsSpecialUser)
			oCondition += getSpecialUserCondition_Master ();

		String oSQL =
		    " Select Distinct en_0651b04_smartmasterinfo.smartid, " +
	            " en_0651b04_smartmasterinfo.mastercode " +
            " From en_0651c02_betstatus_bbl, en_0651b04_smartmasterinfo, en_0651b04_smartgroupinfo WITH (NOLOCK) " +
            " Where en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " +
				" en_0651b04_smartmasterinfo.unitid = " + arrInfo [f_FDCompanyUnitId] + " And " +
	            " en_0651c02_betstatus_bbl.smartid = en_0651b04_smartgroupinfo.smartid And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 " + oCondition + 
				getDateCondition_PB (arrInfo) + 
            " Order By mastercode "; 

        return oSQL;
    }
    
    private String getSmartMasterListSQL_SB (String [] arrInfo)
    {
		String oCondition = "";

        String oFromDate = arrInfo [f_FDFromDate] + " 00:00:00";
        String oToDate = arrInfo [f_FDToDate] + " 23:59:59";        
		boolean bIsSpecialUser = arrInfo [f_FDIsSpecialUser].equals ("1");

		UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
		String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_FDCompanyUnitId]));

		if (oSmartMasterIds_PM.equals ("0") == false)
		{
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
		}

		if (bIsSpecialUser)
			oCondition += getSpecialUserCondition_Master ();

		String oSQL =
			" Select Distinct en_0651b04_smartmasterinfo.smartid, " +
	            " en_0651b04_smartmasterinfo.mastercode " +
            " From en_0651c02_betinfo_bbl, en_0651b04_smartmasterinfo, " +
                " en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts WITH (NOLOCK) " +
            " Where en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " + 
                " en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " + 
                " en_0651b04_smartmasterinfo.unitid = " + arrInfo [f_FDCompanyUnitId] + " And " +               
	            " en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
                " en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 " + oCondition + 
            " Order By mastercode "; 

        return oSQL;
    }
    
	private String getSmartGroupList ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{
		    String [] arrInfo = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (arrInfo [f_FDReportTypeId]);
		
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getSmartGroupListSQL_PB (arrInfo);
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getSmartGroupListSQL_SB (arrInfo);

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_SmartGroups)); 
				oBuffer.append (getStatusXML (T7_SmartGroups, 1, "BetsByGroupBL:getSmartGroupList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T7_SmartGroups, -1, "BetsByGroupBL:getSmartGroupList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T7_SmartGroups, -1, "BetsByGroupBL:getSmartGroupList:" + oException.toString ()));
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
    
    private String getSmartGroupListSQL_PB (String [] arrInfo)
    {
		String oCondition = getDateCondition_PB (arrInfo);
		boolean bIsSpecialUser = arrInfo [f_FDIsSpecialUser].equals ("1");

		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_FDCompanyUnitId]));

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0  ";			
		}

		if (bIsSpecialUser)
			oCondition += getSpecialUserCondition_Group ();

		String oSQL =
			" Select Distinct en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode " +
			" From en_0651c02_betstatus_bbl, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts WITH (NOLOCK) " +
			" Where en_0651c02_betstatus_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 " + oCondition + " And " + 
				" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_FDCompanyUnitId] +
			" Order By groupcode ";
		
        return oSQL;
    }
    
    private String getSmartGroupListSQL_SB (String [] arrInfo)
    {
		String oCondition = "";
        String oFromDate = arrInfo [f_FDFromDate] + " 00:00:00";
        String oToDate = arrInfo [f_FDToDate] + " 23:59:59";        
		boolean bIsSpecialUser = arrInfo [f_FDIsSpecialUser].equals ("1");

		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (arrInfo [f_FDCompanyUnitId]));

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";			
		}

		if (bIsSpecialUser)
			oCondition += getSpecialUserCondition_Group ();

		String oSQL =
			" Select Distinct en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode " +
			" From en_0651c02_betinfo_bbl, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts WITH (NOLOCK) " +
			" Where en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 " + oCondition +  " And " + 
				" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_FDCompanyUnitId] +
			" Order By groupcode ";

        return oSQL;
    }
    
    private String getBetList ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        String [] arrInfo = getParams (kCR_BetList);
        String [] arrMoreFilters = getParams (kCR_MoreFilters);
        int nUnitId = convertToInt (arrInfo [BLConstants.f_CompanyUnitId]);

        BetListObj oBetListObj = new BetListObj (getDocument ());
	    oBuffer.append (oBetListObj.getBetList (SportUtil.kS_BasketBall, arrInfo, arrMoreFilters));
        oBuffer.append (TradingUtil.getUserBLTypePM (this, T8_BLTypePM, kSR_BLTypePM, getUserId (), nUnitId));
        
        return oBuffer.toString ();
	}	
	
    private String getPunterPerformance_A ()
    {
        String [] arrInfo = getParams (kCR_A_PunterPerformance);
        MonthPerformanceObj oLogic = new MonthPerformanceObj (getDocument ());

        String oXMLString = oLogic.getMonthPerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
    }        
    
    private String getDaysPerformance_B ()
    {
        String [] arrInfo = getParams (kCR_B_DayPerformance);
        MonthPerformanceObj oLogic = new MonthPerformanceObj (getDocument ());

        String oXMLString = oLogic.getDaysPerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
    }    

 	private String getDateCondition_PB (String [] arrInfo)
	{
        String oFromDate = arrInfo [f_FDFromDate] + " 00:00:00";
        String oToDate = arrInfo [f_FDToDate] + " 23:59:59";        

		String oCondition = " And DateAdd (Minute, " + ConstantsUtil.kOffsetMinutes + ", en_0651c02_betstatus_bbl.scheduledate) " +
			" Between '" + oFromDate + "' And DateAdd (Minute, " + ConstantsUtil.kExtraMinutes + ", '" + oToDate + "') ";
		
		return oCondition;
	}

	protected String getSpecialUserCondition_Master ()
	{
		String oCondition = " And en_0651b04_smartgroupinfo.mastergroupid In (" +
				" Select groupinfo.mastergroupid " +
				" From en_0651b91_sppwinpunters_bbl, en_0651b04_smartgroupinfo As groupinfo " +
				" Where en_0651b91_sppwinpunters_bbl.smartid = groupinfo.smartid And " + 
					" en_0651b91_sppwinpunters_bbl.typeid = 1 And " +	// WinPunters
					" en_0651b91_sppwinpunters_bbl.userid = " + getUserId () +
			") ";

		return oCondition;
	}

	protected String getSpecialUserCondition_Group ()
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