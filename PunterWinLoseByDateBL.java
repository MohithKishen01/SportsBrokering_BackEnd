 package enlj.p106trading.mssqlv51.p10651basketball.pbpunterwinlosebydate.logics;

import java.sql.*;
import java.util.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;

import enlj.p101admin.commonsv11.resource.logics.userpm.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.bcommons.csmartperffltrs.logics.*;

public class PunterWinLoseByDateBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510411";    	

    /* Win/Lose Type Constants */
    private final int kWLT_WinBets	= 1;
    private final int kWLT_LoseBets	= 2;
	private final int kWLT_DNBBets	= 3;		

	/* Order By Type Constants */
    private final int kOB_WinPercent	= 1;
    private final int kOB_LosePercent	= 2;

	/* Task Ids */
	private final String kInitData		= "10651041101";	
	private final String kMainBetTypes	= "10651041104";
	private final String kLeagues       = "10651041105";
	private final String kSmartMasters	= "10651041106";
	private final String kSmartGroups	= "10651041107";
	private final String kGroupWinLose	= "10651041108";	

	private final String kA_AccountWLByPunter	= "10651041111";

	/* Server Row Names */
	private final String kSR_IsClientLogin	= "sr1";
    private final String kSR_MainBetTypes	= "sr2";
	private final String kSR_Leagues		= "sr3";
	private final String kSR_SmartMasters	= "sr4";
	private final String kSR_SmartGroups	= "sr5";
	private final String kSR_Currencies		= "sr6";
	private final String kSR_GroupWinLose	= "sr7";
	private final String kSR_CompanyUnits	= "sr8";	

	private final String kSR_A_AccountWL	= "sr11";

	/* Client Row Names */	
	private final String kCR_CommonList		= "cr1";
	private final String kCR_GroupWinlose	= "cr2";

	private final String kCR_A_AccountWL	= "cr11";
	
	/* Status Ids */	
	private final String T1_FunctionPM		= "101";
	private final String T1_IsClientLogin	= "102";
	private final String T1_Currencies		= "103";
	private final String T1_CompanyUnits    = "104";

	private final String T4_MainBetTypes	= "401";
	private final String T5_Leagues			= "501";
	private final String T6_SmartMasters	= "601";
	private final String T7_SmartGroups		= "701";
	private final String T8_GroupWinLose	= "801";
	
	private final String T11_AccountWL	= "1101";

	/* Filter Data Fields */
	private final int f_FromDate_CL	    = 0;
	private final int f_ToDate_CL	    = 1;
	private final int f_CompanyUnitId_CL= 2;

	/* Group Win/Lose Filter Fields */
	private final int f_IsClientLogin_GWL	= 0;		
	private final int f_FromDate_GWL		= 1;
	private final int f_ToDate_GWL			= 2;
	private final int f_BetTypeIds_GWL		= 3;
	private final int f_LeagueIds_GWL		= 4;
	private final int f_MasterIds_GWL		= 5;
	private final int f_GroupIds_GWL		= 6;
	private final int f_WinLoseTypeId_GWL	= 7;	
	private final int f_LiveStatusId_GWL	= 8;
	private final int f_CurrencyId_GWL		= 9;
	private final int f_WeekDayId_GWL		= 10;
	private final int f_OrderById_GWL		= 11;
	private final int f_LastMonthValue_GWL	= 12;
	private final int f_TopCount_GWL		= 13;
	private final int f_MinBetCount_GWL		= 14;	
	private final int f_DateRange_GWL		= 15;
	private final int f_CompanyUnitId_GWL   = 16;
    		
	/* Account Win/Lose Filter Fields */
	private final int f_IsClientLogin_AWL	= 0;
	private final int f_MasterIds_AWL		= 1;
	private final int f_FromDate_AWL		= 2;
	private final int f_ToDate_AWL			= 3;
	private final int f_GroupId_AWL			= 4;
	private final int f_LeagueIds_AWL		= 5;
	private final int f_BetTypeIds_AWL		= 6;
	private final int f_WinLoseTypeId_AWL	= 7;	
	private final int f_LiveStatusId_AWL	= 8;
	private final int f_CurrencyId_AWL		= 9;
	private final int f_WeekDayId_AWL		= 10;
	private final int f_OrderById_AWL		= 11;
	private final int f_LastMonthValue_AWL	= 12;
	private final int f_TopCount_AWL		= 13;
	private final int f_MinBetCount_AWL		= 14;	
	private final int f_DateRange_AWL		= 15;
	private final int f_CompanyUnitId_AWL		= 16;

   	public PunterWinLoseByDateBL ()
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
		else if (oTaskId.equals (kMainBetTypes))
        {
            oXMLString = getMainBetTypes ();
        }
		else if (oTaskId.equals (kLeagues))
        {
            oXMLString = getLeagueList ();
        }		
		else if (oTaskId.equals (kSmartMasters))
        {
            oXMLString = getSmartMaster ();
        }
		else if (oTaskId.equals (kSmartGroups))
        {
            oXMLString = getSmartGroupList ();
        }				      
		else if (oTaskId.equals (kGroupWinLose))
        {
            oXMLString = getGroupWinLose ();
        }		
		else if (oTaskId.equals (kA_AccountWLByPunter))
        {
            oXMLString = getAccountWinLose ();
        }

		return oXMLString;
	}
	
	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));
		oBuffer.append (TradingUtil.getClientLoginStatus (this, T1_IsClientLogin, kSR_IsClientLogin));        
        oBuffer.append (TradingUtil.getCurrencies_ByName (this, T1_Currencies, kSR_Currencies, ConstantsUtil.kFE_All));
        oBuffer.append (TradingUtil.getCompanyUnits (this, T1_CompanyUnits, kSR_CompanyUnits, ConstantsUtil.kFE_ChooseOne));

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
					" en_0251z00_bettype_main.mainbettype_" + getLanguage () + " As mainbettype " +
				" From en_0651c02_betinfo_bbl, en_0251z00_bettype_main, en_0251z00_bettype, en_0251b02_accountinfo " +
				" Where en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
					" en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
					" en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
                    " en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_CL] + " And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0251z00_bettype.sportid = " + SportUtil.kS_BasketBall +
				" Order By mainbettypeid ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MainBetTypes)); 
				oBuffer.append (getStatusXML (T4_MainBetTypes, 1, "PunterWinLoseByDateBL:getMainBetTypes:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "PunterWinLoseByDateBL:getMainBetTypes:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "PunterWinLoseByDateBL:getMainBetTypes:" + oException.toString ()));
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
				" From en_0651c02_betinfo_bbl, en_0251b02_accountinfo, en_0651b01_leagueinfo_bbl " +
				" Where en_0651c02_betinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
				    " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
                    " en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_CL] + " And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0651b01_leagueinfo_bbl.leagueid > 0 " +					
				" Order By leaguename ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Leagues)); 
				oBuffer.append (getStatusXML (T5_Leagues, 1, "PunterWinLoseByDateBL:getLeagueList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Leagues, -1, "PunterWinLoseByDateBL:getLeagueList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Leagues, -1, "PunterWinLoseByDateBL:getLeagueList:" + oException.toString ()));
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

	private String getSmartGroupList ()
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
			String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), getModuleId (), SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_CL]));

			if (oSmartMasterIds_PM.equals ("0") == false)
			{
				oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
			}

			UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
			String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), getModuleId (), SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_CL]));

			if (oSmartGroupIds_PM.equals ("0") == false)
			{
				oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
			}
			
			String oSQL =
				" Select Distinct en_0651b04_smartgroupinfo.smartid, " +
					" en_0651b04_smartgroupinfo.groupcode " +
				" From en_0651c02_betinfo_bbl, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts " +
				" Where en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
					" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
					" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CompanyUnitId_CL] + " And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
					" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
					" en_0651b04_smartgroupinfo.isactive = 1 " +
					oCondition +					
				" Order By groupcode ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_SmartGroups)); 
				oBuffer.append (getStatusXML (T7_SmartGroups, 1, "PunterWinLoseByDateBL:getSmartGroupList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T7_SmartGroups, -1, "PunterWinLoseByDateBL:getSmartGroupList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T7_SmartGroups, -1, "PunterWinLoseByDateBL:getSmartGroupList:" + oException.toString ()));
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

	private String getSmartMaster ()
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
			String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), getModuleId (), SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_CL]));

			if (oSmartMasterIds_PM.equals ("0") == false)
			{
				oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
			}

			String oSQL =
				" Select Distinct en_0651b04_smartmasterinfo.smartid, " +
					" en_0651b04_smartmasterinfo.mastercode " +				
				" From en_0651c02_betinfo_bbl, en_0651b04_smartmasterinfo, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts " +
				" Where en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
					" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
					" en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " +
					" en_0651b04_smartmasterinfo.unitid = " + arrInfo [f_CompanyUnitId_CL] + " And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0651b04_smartmasterinfo.sportid = " + SportUtil.kS_BasketBall +
					oCondition +
				" Order By mastercode ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_SmartMasters)); 
				oBuffer.append (getStatusXML (T6_SmartMasters, 1, "PunterWinLoseByDateBL:getSmartMaster:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T6_SmartMasters, -1, "PunterWinLoseByDateBL:getSmartMaster:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T6_SmartMasters, -1, "PunterWinLoseByDateBL:getSmartMaster:" + oException.toString ()));
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

	private String getGroupWinLose ()
	{
		StringBuffer oBuffer = new StringBuffer ("");
		
		try
		{			
			String [] arrInfo = getParams (kCR_GroupWinlose);
			Hashtable hashRecords = new Hashtable ();
			Hashtable hashGroupInfo = new Hashtable ();
			
			ArrayList arrSmartIds = getSmartGroupIds (arrInfo, hashGroupInfo);
			
			String oStatusXML = updateGroupWinLose (arrInfo, hashRecords, hashGroupInfo);
			oBuffer.append (oStatusXML);

			Enumeration oEnum = hashRecords.keys ();
			while (oEnum.hasMoreElements ())
			{
				String oDateKey = oEnum.nextElement ().toString ();
				AmountInfo oAmountInfo = (AmountInfo)hashRecords.get (oDateKey);

				if (arrSmartIds.size () > 0)
				{
					String oRecordInfo = oAmountInfo.getRecordInfo (arrSmartIds);
					if (oRecordInfo.equals ("") == false)
					{
						String oRowData = DataUtil.getRowXML (kSR_GroupWinLose, oRecordInfo);
						oBuffer.append (oRowData);
					}
				}
			}
		}
		catch (Exception oException)
		{
            log ("PunterWinLoseByDateBL:getGroupWinLose:" + oException.toString ());
		}
		
		return oBuffer.toString ();
	}
	
	private String updateGroupWinLose (String [] arrInfo, Hashtable hashRecords, Hashtable hashGroupInfo)
	{
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;						

		String oXMLString = "";

        try
        {						
			updateDateRange_GWL (arrInfo [f_DateRange_GWL], hashRecords, hashGroupInfo);

			String oSQL = getGroupWinLoseSQL (arrInfo);
			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

			if (oResultSet != null)
			{
				while (oResultSet.next ())
				{
					String oDateKey = oResultSet.getString ("datekey");
					if (hashRecords.containsKey (oDateKey))
					{
						String oSmartId = oResultSet.getString ("smartid");
						String oWinLoseInfo = oResultSet.getString ("turnover") + DataUtil.kEXT_SEP + 
							oResultSet.getString ("winlose") + DataUtil.kEXT_SEP + oResultSet.getString ("clientacc_count");
						
						AmountInfo oAmountInfo = (AmountInfo)hashRecords.get (oDateKey);
						oAmountInfo.updateRecordColumnInfo (oSmartId, oWinLoseInfo);
						hashRecords.put (oDateKey, oAmountInfo);
					}
				}
				oXMLString = getStatusXML (T8_GroupWinLose, 1, "PunterWinLoseByDateBL:updateGroupWinLose:Successful");
			}
			else
				oXMLString = getStatusXML (T8_GroupWinLose, -1, "PunterWinLoseByDateBL:updateGroupWinLose:Unsuccessful");
        }
        catch (Exception oException)
        {			
			oXMLString = getStatusXML (T8_GroupWinLose, -1, "PunterWinLoseByDateBL:updateGroupWinLose:Unsuccessful");
            log ("PunterWinLoseByDateBL:getGroupWinLose:" + oException.toString ());
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

		return oXMLString;
	}

	private void updateDateRange_GWL (String oDateRange, Hashtable hashRecords, Hashtable hashGroupInfo)
	{
		String [] arrDates = oDateRange.split ("_");
		for (int nIndex = 0; nIndex < arrDates.length; nIndex++)
		{
			String oDate = arrDates [nIndex];
			hashRecords.put (oDate, new AmountInfo (oDate, hashGroupInfo));
		}
	}
	
	private ArrayList getSmartGroupIds (String [] arrInfo, Hashtable hashGroupInfo)
	{
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;				

		ArrayList arrSmartGroupIds = new ArrayList ();

        try
        {
			String oSQL = getSmartGroupIdsSQL (arrInfo);
			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

			if (oResultSet != null)
			{
				while (oResultSet.next ())
				{
					String oSmartId = oResultSet.getString ("smartid");

					String oGroupInfo = oSmartId + DataUtil.kEXT_SEP + oResultSet.getString ("groupcode") + DataUtil.kEXT_SEP +
						oResultSet.getString ("fgcolor") + DataUtil.kEXT_SEP + oResultSet.getString ("bgcolor") + DataUtil.kEXT_SEP +
						oResultSet.getString ("currencycode");
					
					arrSmartGroupIds.add (oSmartId);
					hashGroupInfo.put (oSmartId, oGroupInfo);
				}
			}
        }
        catch (Exception oException)
        {
            log ("PunterWinLoseByDateBL:getSmartGroupIds:" + oException.toString ());
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

		return arrSmartGroupIds;
	}
	
	private String getSmartGroupIdsSQL (String [] arrInfo)
	{
		String oFromDate = arrInfo [f_FromDate_GWL] + " 00:00:00";
		String oToDate = arrInfo [f_ToDate_GWL] + " 23:59:59";		

		int nRateTypeId = ConstantsUtil.kRateT_GBP;				

		String oSQL = 
			" Select en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode, " +
				" IsNull (en_0651b04_smartgroupinfo.fgcolor, '#000000') As fgcolor, " +
				" IsNull (en_0651b04_smartgroupinfo.bgcolor, '#ffffff') As bgcolor, " +
				" en_0151z00_currency.currencycode_" + getLanguage () + " As currencycode, " +
				" Sum (performanceinfo.turnover / en_0651z00_currencyrates.currencyrate) As turnover, " +
				" Sum (performanceinfo.winlose / en_0651z00_currencyrates.currencyrate) As winlose, " +
				" Sum (performanceinfo.winlose) / Sum (performanceinfo.turnover) As winpercent " +
			" From " +
			" ( " +
				" Select en_0251b02_accountinfo.accountid, " +
					" Sum (en_0651c02_betinfo_bbl.stake * account_currateinfo.currencyrate) As turnover, " +
					" Sum (en_0651c02_betinfo_bbl.winlose * account_currateinfo.currencyrate) As winlose " +	
				" From en_0651c02_betinfo_bbl, en_0251b02_accountinfo, en_0251z00_bettype, " +
					" en_0651z00_currencyrates As account_currateinfo " +
				" Where en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
					" en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
					" account_currateinfo.currencyid = en_0251b02_accountinfo.currencyid And " +
					" en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_GWL] + " And " +
					" account_currateinfo.ratetypeid = " + nRateTypeId + " And " +
					" en_0651c02_betinfo_bbl.settlementby > 0 And " +
					" en_0651c02_betinfo_bbl.confirmby > 0 And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' " +
					getCondition_General (arrInfo) +
				" Group By en_0251b02_accountinfo.accountid " +
			" ) As performanceinfo, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts, " +
				" en_0151z00_currency, en_0651z00_currencyrates WITH (NOLOCK) " +
			" Where en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
				" performanceinfo.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.currencyid = en_0651z00_currencyrates.currencyid And " +
				" en_0651b04_smartgroupinfo.currencyid = en_0151z00_currency.currencyid And " +
				" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CompanyUnitId_GWL] + " And " +
				" en_0651z00_currencyrates.ratetypeid = " + nRateTypeId + " And " +
				" en_0651b04_smartgroupaccounts.isreplicated = 0 And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall +
				getCondition_Smart (arrInfo) +				
			" Group By en_0651b04_smartgroupinfo.smartid, en_0651b04_smartgroupinfo.groupcode, " +
				" IsNull (en_0651b04_smartgroupinfo.fgcolor, '#000000'), " +
				" IsNull (en_0651b04_smartgroupinfo.bgcolor, '#ffffff'), " +
				" en_0151z00_currency.currencycode_" + getLanguage () +
			getOrderByCondition_Group (arrInfo);

		return oSQL;
	}

	private String getGroupWinLoseSQL (String [] arrInfo)
	{
		String oClientIds_PM = getClientIds_PM (arrInfo);
		String oFromDate = arrInfo [f_FromDate_GWL] + " 00:00:00";
		String oToDate = arrInfo [f_ToDate_GWL] + " 23:59:59";
		int nRateTypeId = ConstantsUtil.kRateT_GBP;

		String oSQL = 
			" Select performanceinfo.datekey, " +
				" en_0651b04_smartgroupinfo.smartid, " +
				" Sum (performanceinfo.turnover / en_0651z00_currencyrates.currencyrate) As turnover, " +
				" Sum (performanceinfo.winlose / en_0651z00_currencyrates.currencyrate) As winlose, " +
				" performanceinfo.orderdate, " +
				" performanceinfo.clientacc_count " +
			" From " +
			" ( " +
				" Select en_0251b02_accountinfo.accountid, " +
					" Replace (Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 111), '/', '-') As datekey, " +
					" Sum (en_0651c02_betinfo_bbl.stake * account_currateinfo.currencyrate) As turnover, " +
					" Sum (en_0651c02_betinfo_bbl.winlose * account_currateinfo.currencyrate) As winlose, " +
					" Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 112) As orderdate, " +
					" Sum (Case When en_0251b02_accountinfo.clientid In (-1) Then 1 Else 0 End) As clientacc_count " +
				" From en_0651c02_betinfo_bbl, en_0251b02_accountinfo, en_0251z00_bettype, " +
					" en_0651z00_currencyrates As account_currateinfo WITH (NOLOCK) " +
				" Where en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
					" en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +		
					" account_currateinfo.currencyid = en_0251b02_accountinfo.currencyid And " + 
					" en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_GWL] + " And " +
					" account_currateinfo.ratetypeid = " + nRateTypeId + " And " +
					" en_0651c02_betinfo_bbl.settlementby > 0 And  en_0651c02_betinfo_bbl.confirmby > 0 And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' " +
					getCondition_General (arrInfo) +
				" Group By en_0251b02_accountinfo.accountid, " +
					" Replace (Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 111), '/', '-'), " +
					" Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 112) " +
			" ) As performanceinfo, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts, " +
				" en_0151z00_currency, en_0651z00_currencyrates WITH (NOLOCK) " +
			" Where performanceinfo.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +  	
				" en_0651b04_smartgroupinfo.currencyid = en_0651z00_currencyrates.currencyid And " +
				" en_0651b04_smartgroupinfo.currencyid = en_0151z00_currency.currencyid And " +
				" en_0651b04_smartgroupinfo.unitid = " + arrInfo [f_CompanyUnitId_GWL] + " And " +
				" en_0651z00_currencyrates.ratetypeid = " + nRateTypeId + " And " +
				" en_0651b04_smartgroupaccounts.isreplicated = 0 And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall +
				getCondition_Smart (arrInfo) +
			" Group By performanceinfo.datekey, " +
				" en_0651b04_smartgroupinfo.smartid, " +
				" performanceinfo.orderdate, " +
				" performanceinfo.clientacc_count " +
			" Order By orderdate ";

		return oSQL;
	}

	private String getClientIds_PM (String [] arrInfo)
	{
	    String oClientIds_PM = "-1";
	    int nIsClientLogin = convertToInt (arrInfo [f_IsClientLogin_GWL]);

	    if (nIsClientLogin == 1)
	    {
			UserClientPM_T1 objClientPM = new UserClientPM_T1 (getDocument ());
			oClientIds_PM = objClientPM.getUserClientIds_PM (getUserId (), getModuleId (), convertToInt (arrInfo [f_CompanyUnitId_GWL]));
	    }

	    return oClientIds_PM;
	}

	private String getCondition_Smart (String [] arrInfo)
    {		
		String oMasterIds = arrInfo [f_MasterIds_GWL];
        String oGroupIds = arrInfo [f_GroupIds_GWL];			
						 		
		String oCondition = getTopPerfCondition (arrInfo);

		UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
		String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), getModuleId (), SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_GWL]));

		if (oSmartMasterIds_PM.equals ("0") == false)
		{
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
		}

		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), getModuleId (), SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_GWL]));

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll (", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
		}

		if (oMasterIds.equals ("0") == false) 
			oCondition += " And en_0651b04_smartgroupinfo.mastergroupid In (" + oMasterIds + ") ";
		else
			oCondition += " And en_0651b04_smartgroupinfo.isexcludedpp = 0";

		if (oGroupIds.equals ("0") == false)
			oCondition += " And en_0651b04_smartgroupinfo.smartid In (" + oGroupIds + ") ";        

        return oCondition;
    }

	private String getCondition_General (String [] arrInfo)
    {	
		String oBetTypeIds = arrInfo [f_BetTypeIds_GWL];
		String oLeagueIds = arrInfo [f_LeagueIds_GWL];		
		int nLiveTypeId = convertToInt (arrInfo [f_LiveStatusId_GWL]);
		int nCurrencyId = convertToInt (arrInfo [f_CurrencyId_GWL]);
		int nWeekDay = convertToInt (arrInfo [f_WeekDayId_GWL]);		
						 		
		String oCondition = getWinLoseCondition_Group (arrInfo);

        if (oLeagueIds.equals ("-1") == false)
			oCondition += " And en_0651c02_betinfo_bbl.leagueid In (" + oLeagueIds + ") ";

		if (oBetTypeIds.equals ("0") == false)
			oCondition += " And en_0251z00_bettype.mainbettypeid In (" + oBetTypeIds + ") ";        

		if (nLiveTypeId != -1)
			oCondition += " And en_0651c02_betinfo_bbl.islive = " + nLiveTypeId;

		if (nCurrencyId != 0)
			oCondition += " And en_0151z00_currency.currencyid = " + nCurrencyId;		

		if (nWeekDay > -1)
			oCondition += " And (DatePart (WeekDay, en_0651c02_betinfo_bbl.settlementdate) - 1) = " + nWeekDay;

        return oCondition;
    }

	private String getTopPerfCondition (String [] arrInfo)
	{
		String oCondition = "";

		String oLastMonthValue = arrInfo [f_LastMonthValue_GWL];
		if (oLastMonthValue.equals ("0") == false)
		{
			String [] arrInfo_Perf = 
			{
				arrInfo [f_BetTypeIds_GWL],
				arrInfo [f_LeagueIds_GWL],
				oLastMonthValue,
				arrInfo [f_TopCount_GWL],
				arrInfo [f_MinBetCount_GWL]
			};
			
			SmartPerfFltrsObj objSmartPerfFlters = new SmartPerfFltrsObj (getDocument ());
			String oSmartIds = objSmartPerfFlters.getSmartIds (SportUtil.kS_BasketBall, arrInfo_Perf);

			if (oSmartIds.equals ("-1") == false)
				oCondition = " And en_0651b04_smartgroupinfo.smartid In (" + oSmartIds + ") ";
		}

		return oCondition;
	}

	private String getWinLoseCondition_Group (String [] arrInfo)
    {		
		int nWinLoseTypeId = convertToInt (arrInfo [f_WinLoseTypeId_GWL]);		
		String oCondition = "";

		switch (nWinLoseTypeId)
        {
            case kWLT_WinBets :
                oCondition += " And en_0651c02_betinfo_bbl.winlose > 0 ";
                break;
            case kWLT_LoseBets :
                oCondition += " And en_0651c02_betinfo_bbl.winlose < 0 ";
                break;
            case kWLT_DNBBets :
                oCondition += " And en_0651c02_betinfo_bbl.winlose = 0 ";
                break;
        }
		
		return oCondition;
	}

	private String getOrderByCondition_Group (String [] arrInfo)
    {		
		int nOrderBy = convertToInt (arrInfo [f_OrderById_GWL]);
		String oOrder = "";

		switch (nOrderBy)
        {
            case kOB_WinPercent :
                oOrder += " Order By winpercent Desc ";
                break;
            case kOB_LosePercent :
                oOrder += " Order By winpercent ";
                break;            
        }
		
		return oOrder;
	}
	
	private String getAccountWinLose ()
	{
		StringBuffer oBuffer = new StringBuffer ("");
		
		try
		{			
			String [] arrInfo = getParams (kCR_A_AccountWL);
			Hashtable hashRecords = new Hashtable ();
			Hashtable hashAccountInfo = new Hashtable ();
			
			ArrayList arrAccountIds = getAccountIds (arrInfo, hashAccountInfo);
			
			String oStatusXML = updateAccountWinLose (arrInfo, hashRecords, hashAccountInfo);
			oBuffer.append (oStatusXML);

			Enumeration oEnum = hashRecords.keys ();
			while (oEnum.hasMoreElements ())
			{
				String oDateKey = oEnum.nextElement ().toString ();
				AmountInfo oAmountInfo = (AmountInfo)hashRecords.get (oDateKey);

				if (arrAccountIds.size () > 0)
				{
					String oRecordInfo = oAmountInfo.getRecordInfo (arrAccountIds);
					if (oRecordInfo.equals ("") == false)
					{
						String oRowData = DataUtil.getRowXML (kSR_A_AccountWL, oRecordInfo);
						oBuffer.append (oRowData);
					}
				}
			}
		}
		catch (Exception oException)
		{
            log ("PunterWinLoseByDateBL:getAccountWinLose:" + oException.toString ());
		}
		
		return oBuffer.toString ();
	}
	
	private String updateAccountWinLose (String [] arrInfo, Hashtable hashRecords, Hashtable hashAccountInfo)
	{
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;						

		String oXMLString = "";

        try
        {						
			updateDateRange_AWL (arrInfo [f_DateRange_AWL], hashRecords, hashAccountInfo);

			String oSQL = getAccountWinLoseSQL (arrInfo);
			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

			if (oResultSet != null)
			{
				while (oResultSet.next ())
				{
					String oDateKey = oResultSet.getString ("datekey");
					if (hashRecords.containsKey (oDateKey))
					{
						String oAccountId = oResultSet.getString ("accountid");
						String oWinLoseInfo = oResultSet.getString ("turnover") + DataUtil.kEXT_SEP + 
							oResultSet.getString ("winlose") + DataUtil.kEXT_SEP + oResultSet.getString ("clientacc_count");
						
						AmountInfo oAmountInfo = (AmountInfo)hashRecords.get (oDateKey);
						oAmountInfo.updateRecordColumnInfo (oAccountId, oWinLoseInfo);
						hashRecords.put (oDateKey, oAmountInfo);
					}
				}
				oXMLString = getStatusXML (T11_AccountWL, 1, "PunterWinLoseByDateBL:updateAccountWinLose:Successful");
			}
			else
				oXMLString = getStatusXML (T11_AccountWL, -1, "PunterWinLoseByDateBL:updateAccountWinLose:Unsuccessful");
        }
        catch (Exception oException)
        {			
			oXMLString = getStatusXML (T11_AccountWL, -1, "PunterWinLoseByDateBL:updateAccountWinLose:Unsuccessful");
            log ("PunterWinLoseByDateBL:getAccountWinLose:" + oException.toString ());
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

		return oXMLString;
	}

	private void updateDateRange_AWL (String oDateRange, Hashtable hashRecords, Hashtable hashAccountInfo)
	{
		String [] arrDates = oDateRange.split ("_");
		for (int nIndex = 0; nIndex < arrDates.length; nIndex++)
		{
			String oDate = arrDates [nIndex];
			hashRecords.put (oDate, new AmountInfo (oDate, hashAccountInfo));
		}
	}
	
	private ArrayList getAccountIds (String [] arrInfo, Hashtable hashAccountInfo)
	{
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;				

		ArrayList arrAccountIds = new ArrayList ();

        try
        {
			String oSQL = getAccountIdsSQL (arrInfo);
			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

			if (oResultSet != null)
			{
				while (oResultSet.next ())
				{
					String oAccountId = oResultSet.getString ("accountid");

					String oAccountInfo = oAccountId + DataUtil.kEXT_SEP + oResultSet.getString ("accountcode") + DataUtil.kEXT_SEP +						
						oResultSet.getString ("currencycode");
					
					arrAccountIds.add (oAccountId);
					hashAccountInfo.put (oAccountId, oAccountInfo);
				}
			}
        }
        catch (Exception oException)
        {
            log ("PunterWinLoseByDateBL:getAccountIds:" + oException.toString ());
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

		return arrAccountIds;
	}
	
	private String getAccountIdsSQL (String [] arrInfo)
	{
		String oFromDate = arrInfo [f_FromDate_AWL] + " 00:00:00";
		String oToDate = arrInfo [f_ToDate_AWL] + " 23:59:59";		
		int nSmartId = convertToInt (arrInfo [f_GroupId_AWL]);

		int nRateTypeId = ConstantsUtil.kRateT_GBP;				

		String oSQL = 
			" Select en_0251b02_accountinfo.accountid, " +
				" en_0251b02_accountinfo.accountcode, " +
				" en_0151z00_currency.currencycode_" + getLanguage () + " As currencycode, " +
				" Sum (en_0651c02_betinfo_bbl.stake * account_currateinfo.currencyrate / group_currateinfo.currencyrate) As turnover, " +
				" Sum (en_0651c02_betinfo_bbl.winlose * account_currateinfo.currencyrate / group_currateinfo.currencyrate) As winlose, " +
				" Sum (en_0651c02_betinfo_bbl.winlose * account_currateinfo.currencyrate) / Sum (en_0651c02_betinfo_bbl.stake * account_currateinfo.currencyrate) As winpercent " + 
			" From en_0651c02_betinfo_bbl, en_0251b02_accountinfo, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts, en_0251z00_bettype, " +
				" en_0151z00_currency, en_0651z00_currencyrates As account_currateinfo, en_0651z00_currencyrates As group_currateinfo " +
			" Where en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
				" en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +  
				" en_0251b02_accountinfo.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.currencyid = group_currateinfo.currencyid And " +
				" en_0251b02_accountinfo.currencyid = en_0151z00_currency.currencyid And " + 
				" account_currateinfo.currencyid = en_0251b02_accountinfo.currencyid And " +
				" account_currateinfo.ratetypeid = " + nRateTypeId + " And " +
				" group_currateinfo.ratetypeid = " + nRateTypeId + " And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 And " +
				" en_0651c02_betinfo_bbl.confirmby > 0 And " +
				" en_0651b04_smartgroupaccounts.isreplicated = 0 And " +
				" en_0651b04_smartgroupinfo.smartid = " + nSmartId + " And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_AWL] + " And " +
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' " +
				getAccountPerfCondition (arrInfo) +				
			" Group By en_0251b02_accountinfo.accountid, " +
				" en_0251b02_accountinfo.accountcode, " +
				" en_0151z00_currency.currencycode_" + getLanguage () +
			getOrderByCondition_Acc (arrInfo);

		return oSQL;
	}

	private String getAccountWinLoseSQL (String [] arrInfo)
	{
		String oClientIds_PM = getClientIds_PM_Acc (arrInfo);
		String oFromDate = arrInfo [f_FromDate_AWL] + " 00:00:00";
		String oToDate = arrInfo [f_ToDate_AWL] + " 23:59:59";
		int nSmartId = convertToInt (arrInfo [f_GroupId_AWL]);

		int nRateTypeId = ConstantsUtil.kRateT_GBP;

		String oSQL = 
			" Select Replace (Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 111), '/', '-') As datekey, " +				
				" en_0251b02_accountinfo.accountid, " +
				" Sum (en_0651c02_betinfo_bbl.stake * account_currateinfo.currencyrate / group_currateinfo.currencyrate) As turnover, " +
				" Sum (en_0651c02_betinfo_bbl.winlose * account_currateinfo.currencyrate / group_currateinfo.currencyrate) As winlose, " +
				" Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 112) As orderdate, " +
				" Sum (Case When en_0251b02_accountinfo.clientid In (" + oClientIds_PM + ") Then 1 Else 0 End) As clientacc_count " +
			" From en_0651c02_betinfo_bbl, en_0251b02_accountinfo, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts, en_0251z00_bettype, " +
				" en_0151z00_currency, en_0651z00_currencyrates As account_currateinfo, en_0651z00_currencyrates As group_currateinfo " +
			" Where en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
				" en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +  
				" en_0251b02_accountinfo.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.currencyid = group_currateinfo.currencyid And " +
				" en_0251b02_accountinfo.currencyid = en_0151z00_currency.currencyid And " + 
				" account_currateinfo.currencyid = en_0251b02_accountinfo.currencyid And " +
				" account_currateinfo.ratetypeid = " + nRateTypeId + " And " +
				" group_currateinfo.ratetypeid = " + nRateTypeId + " And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 And " +
				" en_0651c02_betinfo_bbl.confirmby > 0 And " +
				" en_0651b04_smartgroupaccounts.isreplicated = 0 And " +
				" en_0651b04_smartgroupinfo.smartid = " + nSmartId + " And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_AWL] + " And " +
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' " +
				getAccountPerfCondition (arrInfo) +				
			" Group By Replace (Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 111), '/', '-'), " +				
				" en_0251b02_accountinfo.accountid, " +
				" Convert (nvarchar, en_0651c02_betinfo_bbl.settlementdate, 112) " +
			" Order By orderdate ";

		return oSQL;
	}

	private String getClientIds_PM_Acc (String [] arrInfo)
	{
	    String oClientIds_PM = "-1";
	    int nIsClientLogin = convertToInt (arrInfo [f_IsClientLogin_AWL]);

	    if (nIsClientLogin == 1)
	    {
			UserClientPM_T1 objClientPM = new UserClientPM_T1 (getDocument ());
			oClientIds_PM = objClientPM.getUserClientIds_PM (getUserId (), getModuleId (), convertToInt (arrInfo [f_CompanyUnitId_AWL]));
	    }

	    return oClientIds_PM;
	}

	private String getAccountPerfCondition (String [] arrInfo)
    {	
		String oMasterIds = arrInfo [f_MasterIds_AWL];
		String oLeagueIds = arrInfo [f_LeagueIds_AWL];
		String oBetTypeIds = arrInfo [f_BetTypeIds_AWL];        
		int nLiveTypeId = convertToInt (arrInfo [f_LiveStatusId_AWL]);
		int nCurrencyId = convertToInt (arrInfo [f_CurrencyId_AWL]);
		int nWeekDay = convertToInt (arrInfo [f_WeekDayId_AWL]);
						 		
		String oCondition = getWinLoseCondition_Acc (arrInfo);
		oCondition += getTopPerfCondition_Acc (arrInfo);
					
		UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
		String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), getModuleId (), SportUtil.kS_BasketBall, convertToInt (arrInfo [f_CompanyUnitId_AWL]));		

		if (oSmartMasterIds_PM.equals ("0") == false)
		{
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
		}

		if (oMasterIds.equals ("0") == false) 
			oCondition += " And en_0651b04_smartgroupinfo.mastergroupid In (" + oMasterIds + ") ";
		else
			oCondition += " And en_0651b04_smartgroupinfo.isexcludedpp = 0";					

        if (oLeagueIds.equals ("-1") == false)
			oCondition += " And en_0651c02_betinfo_bbl.leagueid In (" + oLeagueIds + ") ";

		if (oBetTypeIds.equals ("0") == false)
			oCondition += " And en_0251z00_bettype.mainbettypeid In (" + oBetTypeIds + ") ";        

		if (nLiveTypeId != -1)
			oCondition += " And en_0651c02_betinfo_bbl.islive = " + nLiveTypeId;		

		if (nCurrencyId != 0)
			oCondition += " And en_0151z00_currency.currencyid = " + nCurrencyId;
	
		if (nWeekDay > -1)
			oCondition += " And (DatePart (WeekDay, en_0651c02_betinfo_bbl.settlementdate) - 1) = " + nWeekDay;

        return oCondition;
    }

	private String getTopPerfCondition_Acc (String [] arrInfo)
	{
		String oCondition = "";

		String oLastMonthValue = arrInfo [f_LastMonthValue_AWL];
		if (oLastMonthValue.equals ("0") == false)
		{
			String [] arrInfo_Perf = 
			{
				arrInfo [f_BetTypeIds_AWL],
				arrInfo [f_LeagueIds_AWL],
				oLastMonthValue,
				arrInfo [f_TopCount_AWL],
				arrInfo [f_MinBetCount_AWL]
			};
			
			SmartPerfFltrsObj objSmartPerfFlters = new SmartPerfFltrsObj (getDocument ());
			String oSmartIds = objSmartPerfFlters.getSmartIds (SportUtil.kS_BasketBall, arrInfo_Perf);

			if (oSmartIds.equals ("-1") == false)
				oCondition = " And en_0651b04_smartgroupinfo.smartid In (" + oSmartIds + ") ";
		}

		return oCondition;
	}

	private String getWinLoseCondition_Acc (String [] arrInfo)
    {		
		int nWinLoseTypeId = convertToInt (arrInfo [f_WinLoseTypeId_AWL]);		
		String oCondition = "";

		switch (nWinLoseTypeId)
        {
            case kWLT_WinBets :
                oCondition += " And en_0651c02_betinfo_bbl.winlose > 0 ";
                break;
            case kWLT_LoseBets :
                oCondition += " And en_0651c02_betinfo_bbl.winlose < 0 ";
                break;
            case kWLT_DNBBets :
                oCondition += " And en_0651c02_betinfo_bbl.winlose = 0 ";
                break;
        }
		
		return oCondition;
	}

	private String getOrderByCondition_Acc (String [] arrInfo)
    {		
		int nOrderBy = convertToInt (arrInfo [f_OrderById_AWL]);
		String oOrder = "";

		switch (nOrderBy)
        {
            case kOB_WinPercent :
                oOrder += " Order By winpercent Desc ";
                break;
            case kOB_LosePercent :
                oOrder += " Order By winpercent ";
                break;            
        }
		
		return oOrder;
	}

	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}
