package enlj.p106trading.mssqlv51.p10651basketball.rbbetsbyteam.logics;

import java.sql.*;
import org.w3c.dom.*;
import java.math.BigInteger;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p101admin.commonsv11.resource.logics.userpm.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.estatistics.gteamstats.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.bcommons.csmartperffltrs.logics.*;

public class BetsByTeamBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510408";    	

    /* Report Type Constants */
	private final int kRT_PendingBets	= 1;
	private final int kRT_SettledBets	= 2;
    
	/* Task Ids */
	private final String kInitData	    = "10651040801";
	private final String kMainBetTypes  = "10651040804";
 	private final String kLeagues       = "10651040805";
 	private final String kSmartMasters  = "10651040806";
 	private final String kSmartGroups   = "10651040807";
 	private final String kBetList		= "10651040808";
 	private final String kBetList_TH	= "10651040809";

	BigInteger kStatsTaskIds_Start = null;
	BigInteger kStatsTaskIds_End = null;

	BigInteger kBetsTaskIds_Start = null;
	BigInteger kBetsTaskIds_End = null;
 	
	/* Server Row Names */
	private final String kSR_AHWeightage_BLF	= "sr1";
    private final String kSR_AHPriceing_BLF		= "sr2";
    private final String kSR_TWeightage_BLF		= "sr3";
    private final String kSR_MTRule_BLF			= "sr4";
    private final String kSR_MainBetTypes		= "sr5";
    private final String kSR_Leagues			= "sr6";
    private final String kSR_SmartMasters		= "sr7";
    private final String kSR_SmartGroups		= "sr8";
    private final String kSR_TimeStamp			= "sr9";
    private final String kSR_MatchList			= "sr10";
    private final String kSR_BetList			= "sr11";
    private final String kSR_FinishedMatchIds	= "sr12";
	private final String kSR_SpecialUserStatus	= "sr13";
	private final String kSR_CompanyUnits		= "sr14";
    
	/* Client Row Names */
    private final String kCR_CommonData		    = "cr1";
    private final String kCR_MainFilters_BBT    = "cr2";
    private final String kCR_MoreFilters_BBT    = "cr3";
    private final String kCR_AdvancedFltr_BBT   = "cr4";
    
    private final String kCR_A_TeamStats_Home   = "cr13";
    private final String kCR_A_TeamStats_Away   = "cr14";
    
	/* Status Ids */		
	private final String T1_FunctionPM			= "101";	
	private final String T1_AHWeightage_BLF		= "102";
	private final String T1_AHPricing_BLF		= "103";
	private final String T1_TWeightage_BLF		= "104";
    private final String T1_MTRule_BLR			= "105";
	private final String T1_SpecialUserStatus	= "106";
	private final String T1_CompanyUnits		= "107";

    private final String T4_MainBetTypes	= "401";
	private final String T5_Leagues	        = "501";
	private final String T6_SmartMasters    = "601";
	private final String T7_SmartGroups	    = "701";

	private final String T8_TimeStamp		= "801";
	private final String T8_MatchList		= "802";
	private final String T8_BetList			= "803";
	private final String T8_FinishedMatchIds= "804";

	/* Common Filter Index */	
	private final int f_ReportTypeId_CF		= 0;
	private final int f_FromDate_CF			= 1;
	private final int f_ToDate_CF			= 2;
	private final int f_ScheduleIds_CF		= 3;
	private final int f_IsSpecialUser_CF	= 4;
	private final int f_CompanyUnitId_CF	= 5;

	/* Main Filter Index */	
	private final int f_SmartMasterIds_MA	= 0;
	private final int f_SmartGroupIds_MA	= 1;
	private final int f_LeagueIds_MA		= 2;
	private final int f_MainBetTypeIds_MA	= 3;
	private final int f_CurrentTimeStamp_MA	= 4;

	/* More Filter Index */	
	private final int f_WinLoseTypeId_MF	= 0;
	private final int f_OrderById_MF	    = 1;
	private final int f_LiveStatusId_MF		= 2;
	private final int f_VIPTypeId_MF	    = 3;
	private final int f_LastMonthId_MF	    = 4;
	private final int f_TopPerformanceId_MF	= 5;
	private final int f_MinimumBetsId_MF	= 6;
	private final int f_SelectionBTId_MF	= 7;
	private final int f_SelectionId_MF	    = 8;

	/* Advanced Filter Index */	
	private final int f_AHWeightageId_BLF_AF	= 0;
	private final int f_AHPricingId_BLF_AF	    = 1;
	private final int f_TWeightageId_BLF_AF	    = 2;
	private final int f_MTRuleId_BLF_AF	        = 3;	

    StatisticsBL m_oStatisticsBL = null;
    BetListBL m_oBetListBL = null;

    String [] m_arrInfo_CF = null;
    String [] m_arrInfo_MA = null;
    String [] m_arrInfo_MF = null;
    String [] m_arrInfo_AF = null;

   	public BetsByTeamBL ()
	{
		super ();
		m_oStatisticsBL = new StatisticsBL ();		
		m_oBetListBL = new BetListBL ();

		kStatsTaskIds_Start = new BigInteger ("10651040811");
	    kStatsTaskIds_End = new BigInteger ("10651040870");

		kBetsTaskIds_Start = new BigInteger ("10651040871");
	    kBetsTaskIds_End = new BigInteger ("10651040899");
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
            oXMLString = getMainBetTypes ();
        }
		else if (oTaskId.equals (kLeagues))
        {
            oXMLString = getLeagues ();
        }
        else if (oTaskId.equals (kSmartMasters))
        {
            oXMLString = getSmartMasters ();
        }
        else if (oTaskId.equals (kSmartGroups))
        {
            oXMLString = getSmartGroups ();
        }
        else if (oTaskId.equals (kBetList))
        {
            oXMLString = processBetList ();
        }
        else if (oTaskId.equals (kBetList_TH))
        {
            oXMLString = processBetList_TH ();
        }
        else if (oTaskId.equals (StatsConstants.kZ_InitData))
        {
            oXMLString = getInitData_Z ();
        }
        else if (oTaskId.equals (StatsConstants.kZ_SeasonList))
        {
            oXMLString = getSeasonData_Z ();
        }
		else if (oTaskId.equals (StatsConstants.kZ_TeamList))
        {
            oXMLString = getTeamData_Z ();
        }
        else if ((biTaskId.compareTo (kStatsTaskIds_Start) == 1 || biTaskId.compareTo (kStatsTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kStatsTaskIds_End) == -1 || biTaskId.compareTo (kStatsTaskIds_End) == 0))
		{
            oXMLString = m_oStatisticsBL.executeTask (oDocument, oTaskId);
        }
        else if ((biTaskId.compareTo (kBetsTaskIds_Start) == 1 || biTaskId.compareTo (kBetsTaskIds_Start) == 0) && 
			(biTaskId.compareTo (kBetsTaskIds_End) == -1 || biTaskId.compareTo (kBetsTaskIds_End) == 0))
		{
            oXMLString = m_oBetListBL.executeTask (oDocument, oTaskId);
        }
		
		return oXMLString;
	}		
    
	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));
		oBuffer.append (getWeightage_HDP ());
		oBuffer.append (getPricing_HDP ());
		oBuffer.append (getWeightage_OU ());
		oBuffer.append (getMTRule ());
		oBuffer.append (BasketballUtil.checkSpecialUser_SPP (this, T1_SpecialUserStatus, kSR_SpecialUserStatus));
        oBuffer.append (TradingUtil.getCompanyUnits (this, T1_CompanyUnits, kSR_CompanyUnits, ConstantsUtil.kFE_ChooseOne));

        return oBuffer.toString ();
    }

    private String getWeightage_HDP ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {		    
		    String oSQL = 
		        " Select en_0651b91_statsmarking.markid, " +
		            " en_0651b91_statsmarking.markname, " +
		            " en_0651b91_statsmarking.bgcolor, " +
		            " en_0651b91_statsmarking.fgcolor, " +
		            " en_0651b91_statsmarking.orderid " +
		        " From en_0651b91_statsmarking " +
		        " Order By en_0651b91_statsmarking.orderid ";
			
			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_AHWeightage_BLF)); 
				oBuffer.append (getStatusXML (T1_AHWeightage_BLF, 1, "BetsByTeamBL:getWeightage_HDP:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T1_AHWeightage_BLF, -1, "BetsByTeamBL:getWeightage_HDP:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T1_AHWeightage_BLF, -1, "BetsByTeamBL:getWeightage_HDP:" + oException.toString ()));
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
    
    private String getPricing_HDP ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {		    
		    String oSQL = 
		        " Select en_0651b91_statsmarking_lbl.lbl_markid, " +
		            " en_0651b91_statsmarking_lbl.lbl_markname, " +
		            " en_0651b91_statsmarking_lbl.bgcolor, " +
		            " en_0651b91_statsmarking_lbl.fgcolor, " +
		            " en_0651b91_statsmarking_lbl.orderid " +
		        " From en_0651b91_statsmarking_lbl " +
		        " Order By en_0651b91_statsmarking_lbl.orderid ";
			
			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_AHPriceing_BLF)); 
				oBuffer.append (getStatusXML (T1_AHPricing_BLF, 1, "BetsByTeamBL:getPricing_HDP:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T1_AHPricing_BLF, -1, "BetsByTeamBL:getPricing_HDP:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T1_AHPricing_BLF, -1, "BetsByTeamBL:getPricing_HDP:" + oException.toString ()));
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
    
    private String getWeightage_OU ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {		    
		    String oSQL = 
		        " Select en_0651b91_statsmarking_kp.kp_markid, " +
		            " en_0651b91_statsmarking_kp.kp_markname, " +
		            " en_0651b91_statsmarking_kp.bgcolor, " +
		            " en_0651b91_statsmarking_kp.fgcolor, " +
		            " en_0651b91_statsmarking_kp.orderid " +
		        " From en_0651b91_statsmarking_kp " +
		        " Order By en_0651b91_statsmarking_kp.orderid ";
			
			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_TWeightage_BLF)); 
				oBuffer.append (getStatusXML (T1_TWeightage_BLF, 1, "BetsByTeamBL:getWeightage_OU:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T1_TWeightage_BLF, -1, "BetsByTeamBL:getWeightage_OU:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T1_TWeightage_BLF, -1, "BetsByTeamBL:getWeightage_OU:" + oException.toString ()));
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
    	
	private String getMTRule ()
    {       
        DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {
            String oSQL =            
                 " Select en_0651b91_statsmarking_mtrule.mtrule_markid, " +
		            " en_0651b91_statsmarking_mtrule.mtrule_markname, " +
		            " en_0651b91_statsmarking_mtrule.bgcolor, " +
		            " en_0651b91_statsmarking_mtrule.fgcolor, " +
		            " en_0651b91_statsmarking_mtrule.orderid " +
		        " From en_0651b91_statsmarking_mtrule " +
		        " Order By en_0651b91_statsmarking_mtrule.orderid ";
				                        
			oStatement = oConnector.getStatement ();			
			
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_MTRule_BLF));
                oBuffer.append (getStatusXML (T1_MTRule_BLR, 1, "BetsByTeamBL:getMTRule:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T1_MTRule_BLR, -1, "BetsByTeamBL:getMTRule:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T1_MTRule_BLR, -1, "BetsByTeamBL:getMTRule" + oException.toString ()));
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
            m_arrInfo_CF = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);
		    
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getMainBetTypesSQL_PB ();
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getMainBetTypesSQL_SB ();

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MainBetTypes)); 
				oBuffer.append (getStatusXML (T4_MainBetTypes, 1, "BetsByTeamBL:getMainBetTypes:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "BetsByTeamBL:getMainBetTypes:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "BetsByTeamBL:getMainBetTypes:" + oException.toString ()));
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
	
	private String getMainBetTypesSQL_PB ()
    {
		String oSQL =
			" Select Distinct en_0251z00_bettype_main.mainbettypeid, " +
				" en_0251z00_bettype_main.mainbettype_" + getLanguage () + " As mainbettype " +
			" From en_0251z00_bettype_main, en_0251z00_bettype, " +
				" en_0651c02_betstatus_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
			    " en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
		        " en_0651c02_betstatus_bbl.accountid = en_0251b02_accountinfo.accountid And " +
				" en_0251b02_accountinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				getDateCondition_PB ("en_0651c02_betstatus_bbl") + 
			" Order By mainbettype ";

        return oSQL;
    }
    
    private String getMainBetTypesSQL_SB ()
    {
        String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";        

		String oSQL =
			" Select Distinct en_0251z00_bettype_main.mainbettypeid, " +
				" en_0251z00_bettype_main.mainbettype_" + getLanguage () + " As mainbettype " +
			" From en_0251z00_bettype_main, en_0251z00_bettype, " +
				" en_0651c02_betinfo_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
			    " en_0251z00_bettype.bettypeid = en_0651c02_betinfo_bbl.bettypeid And " + 
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
                " en_0651c02_betinfo_bbl.settlementby > 0 And " +
		        " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
				" en_0251b02_accountinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
			" Order By mainbettype ";

        return oSQL;
    }    
    
	private String getLeagues ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
        {			
            m_arrInfo_CF = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);
		    
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getLeaguesSQL_PB ();
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getLeaguesSQL_SB ();

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Leagues)); 
				oBuffer.append (getStatusXML (T5_Leagues, 1, "BetsByTeamBL:getLeagues:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Leagues, -1, "BetsByTeamBL:getLeagues:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Leagues, -1, "BetsByTeamBL:getLeagues:" + oException.toString ()));
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
    
    private String getLeaguesSQL_PB ()
    {
		String oSQL =
			" Select Distinct en_0651b01_leagueinfo_bbl.leagueid, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename, " +
				" 0 As ismainleague " + 
			" From en_0651b08_scheduleinfo_bbl, en_0651b01_leagueinfo_bbl WITH (NOLOCK) " +
			" Where en_0651b08_scheduleinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
				" en_0651b01_leagueinfo_bbl.leagueid > 0 " +
				getDateCondition_PB ("en_0651b08_scheduleinfo_bbl") + 
			" Order By leaguename ";

        return oSQL;
    }
    
    private String getLeaguesSQL_SB ()
    {
        String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";        

		String oSQL =
			" Select Distinct en_0651c02_betinfo_bbl.leagueid, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename, " +
				" 0 As ismainleague " + 
			" From en_0651c02_betinfo_bbl, en_0651b01_leagueinfo_bbl WITH (NOLOCK), en_0251b02_accountinfo " +
			" Where en_0651c02_betinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
				" en_0651b01_leagueinfo_bbl.leagueid > 0 And " +
                " en_0651c02_betinfo_bbl.settlementby > 0 And " +
		        " en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
				" en_0251b02_accountinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
			" Order By leaguename ";
		
        return oSQL;
    }    

    private String getSmartMasters ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;

        StringBuffer oBuffer = new StringBuffer ();

        try
        {
		    m_arrInfo_CF = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);
		
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getSmartMastersSQL_PB ();
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getSmartMastersSQL_SB ();		               

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);

            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_SmartMasters));
                oBuffer.append (getStatusXML (T6_SmartMasters, 1, "BetsByTeamBL:getSmartMasters:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T6_SmartMasters, -1, "BetsByTeamBL:getSmartMasters:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T6_SmartMasters, -1, "BetsByTeamBL:getSmartMasters" + oException.toString ()));
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
    
    private String getSmartMastersSQL_PB ()
    {
        String oConditionPM = "";
		boolean bIsSpecialUser = m_arrInfo_CF [f_IsSpecialUser_CF].equals ("1");

        UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
        String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (m_arrInfo_CF [f_CompanyUnitId_CF]));

        if (oSmartMasterIds_PM.equals ("0") == false)
        {
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oConditionPM += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
        }

		if (bIsSpecialUser)
			oConditionPM += getSpecialUserCondition_Master ();

		String oSQL =
		    " Select Distinct en_0651b04_smartmasterinfo.smartid, " +
	            " en_0651b04_smartmasterinfo.mastercode " +
            " From en_0651c02_betstatus_bbl, en_0651b04_smartmasterinfo, en_0651b04_smartgroupinfo WITH (NOLOCK) " +
            " Where en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " + 
	            " en_0651c02_betstatus_bbl.smartid = en_0651b04_smartgroupinfo.smartid And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 And " +
				" en_0651b04_smartmasterinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				getDateCondition_PB ("en_0651c02_betstatus_bbl") + oConditionPM +
            " Order By mastercode "; 

        return oSQL;
    }
    
    private String getSmartMastersSQL_SB ()
    {
        String oConditionPM = "";
        String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";
		boolean bIsSpecialUser = m_arrInfo_CF [f_IsSpecialUser_CF].equals ("1");

        UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
        String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (m_arrInfo_CF [f_CompanyUnitId_CF]));

        if (oSmartMasterIds_PM.equals ("0") == false)
        {
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oConditionPM += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
        }

		if (bIsSpecialUser)
			oConditionPM += getSpecialUserCondition_Master ();

		String oSQL =
			" Select Distinct en_0651b04_smartmasterinfo.smartid, " +
	            " en_0651b04_smartmasterinfo.mastercode " +
            " From en_0651c02_betinfo_bbl, en_0651b04_smartmasterinfo, " +
                " en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts WITH (NOLOCK) " +
            " Where en_0651b04_smartmasterinfo.smartid = en_0651b04_smartgroupinfo.mastergroupid And " + 
                " en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +                
	            " en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
                " en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 And " +
				" en_0651b04_smartmasterinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				oConditionPM +
            " Order By mastercode "; 

        return oSQL;
    }
    
	private String getSmartGroups ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{
		    m_arrInfo_CF = getParams (kCR_CommonData);
		    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);
		
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getSmartGroupsSQL_PB ();
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getSmartGroupsSQL_SB ();

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_SmartGroups)); 
				oBuffer.append (getStatusXML (T7_SmartGroups, 1, "BetsByTeamBL:getSmartGroups:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T7_SmartGroups, -1, "BetsByTeamBL:getSmartGroups:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T7_SmartGroups, -1, "BetsByTeamBL:getSmartGroups:" + oException.toString ()));
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
    
    private String getSmartGroupsSQL_PB ()
    {
        String oConditionPM = "";
		boolean bIsSpecialUser = m_arrInfo_CF [f_IsSpecialUser_CF].equals ("1");

		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (m_arrInfo_CF [f_CompanyUnitId_CF]));			

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
			oConditionPM += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
		}

		if (bIsSpecialUser)
			oConditionPM += getSpecialUserCondition_Group ();

		String oSQL =
			" Select Distinct en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode " +
			" From en_0651c02_betstatus_bbl, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts WITH (NOLOCK) " +
			" Where en_0651c02_betstatus_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 And " +
				" en_0651b04_smartgroupinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				getDateCondition_PB ("en_0651c02_betstatus_bbl") + oConditionPM +
			" Order By groupcode ";

        return oSQL;
    }

    private String getSmartGroupsSQL_SB ()
    {
        String oConditionPM = "";
        String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";
		boolean bIsSpecialUser = m_arrInfo_CF [f_IsSpecialUser_CF].equals ("1");
		    
		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (m_arrInfo_CF [f_CompanyUnitId_CF]));			

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
			oConditionPM += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
		}

		if (bIsSpecialUser)
			oConditionPM += getSpecialUserCondition_Group ();

		String oSQL =
			" Select Distinct en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode " +
			" From en_0651c02_betinfo_bbl, en_0651b04_smartgroupinfo, en_0651b04_smartgroupaccounts WITH (NOLOCK) " +
			" Where en_0651c02_betinfo_bbl.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupinfo.smartid = en_0651b04_smartgroupaccounts.smartid And " +
				" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +  
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.isactive = 1 And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 And " +
				" en_0651b04_smartgroupinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				oConditionPM +
			" Order By groupcode ";
		
        return oSQL;
    }
    	
    private String processBetList ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        m_arrInfo_CF = getParams (kCR_CommonData);
        m_arrInfo_MA = getParams (kCR_MainFilters_BBT);
        m_arrInfo_MF = getParams (kCR_MoreFilters_BBT);
        m_arrInfo_AF = getParams (kCR_AdvancedFltr_BBT);

		oBuffer.append (getCurrentTimeStamp ());
		oBuffer.append (getMatchList (false));
		oBuffer.append (getBetList (false));
		
        return oBuffer.toString ();
    }	
    
    private String processBetList_TH ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        m_arrInfo_CF = getParams (kCR_CommonData);
        m_arrInfo_MA = getParams (kCR_MainFilters_BBT);
        m_arrInfo_MF = getParams (kCR_MoreFilters_BBT);
        m_arrInfo_AF = getParams (kCR_AdvancedFltr_BBT);

		oBuffer.append (getCurrentTimeStamp ());
		oBuffer.append (getMatchList (true));
		oBuffer.append (getBetList (true));
		oBuffer.append (getFinishedMatchIds ());
		
        return oBuffer.toString ();
    }	

    private String getMatchList (boolean bThread)
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{
		    String oSQL = "";
		    
			int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);
			
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getMatchListSQL_PB (bThread);
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getMatchListSQL_SB ();

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MatchList)); 
				oBuffer.append (getStatusXML (T8_MatchList, 1, "BetsByTeamBL:getMatchList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T8_MatchList, -1, "BetsByTeamBL:getMatchList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T8_MatchList, -1, "BetsByTeamBL:getMatchList:" + oException.toString ()));
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
	
    private String getMatchListSQL_PB (boolean bThread)
    {
        String oLeagueIds = m_arrInfo_MA [f_LeagueIds_MA];
		String oCondition = getDateCondition_PB ("en_0651b08_scheduleinfo_bbl");
		
		if (oLeagueIds.equals ("0") == false)
			oCondition += " And en_0651b08_scheduleinfo_bbl.leagueid In  (" + oLeagueIds + ") ";
		
	    String oSQL = 
			" Select en_0651b08_scheduleinfo_bbl.scheduleid, " +
				" en_0651b08_scheduleinfo_bbl.ref_scheduleid, " +
				" Convert (nvarchar, en_0651b08_scheduleinfo_bbl.scheduledate, 103) As scheduledate, " +
				" Convert (nvarchar (5), en_0651b08_scheduleinfo_bbl.scheduledate, 108) As scheduletime, " +  
				" en_0651b06_seasoninfo_bbl.seasonid, " +  
				" en_0651b06_seasoninfo_bbl.seasonname_" + getLanguage () + ", " +  
				" en_0651b01_leagueinfo_bbl.leagueid, " +  
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + ", " +  
				" 0 As ismainleague, " +  
				" en_0651b01_leagueinfo_bbl.iscup, " +  
				" en_0651b01_leagueinfo_bbl.countryid, " +  
				" hometeaminfo.teamid As ateamid, " +  
				" hometeaminfo.teamname_" + getLanguage () + " As ateamname_" + getLanguage () + ", " +  
				" awayteaminfo.teamid As bteamid, " +  
				" awayteaminfo.teamname_" + getLanguage () + " As bteamname_" + getLanguage () + ", " +  
				" en_0651b08_scheduleinfo_bbl.islive, " +  
				" en_0651b08_scheduleinfo_bbl.isneutral, " +  
				" en_0651z00_eventinchargetype.inchargetypeid, " +  
				" en_0651z00_eventinchargetype.inchargecode, " +  
				" scheduleuserinfo.logincode As scheduledby, " +  
				" en_0651b09_openpriceinfo_bbl.ftah_ahandicap As ahandicap, " +  
				" en_0651b09_openpriceinfo_bbl.ftah_bhandicap As bhandicap, " +  
				" en_0651b09_openpriceinfo_bbl.ftah_aprice, " +  
				" en_0651b09_openpriceinfo_bbl.ftah_bprice, " +  
				" en_0651b09_openpriceinfo_bbl.ftou_handicap As ouhandicap, " +  
				" en_0651b09_openpriceinfo_bbl.ftou_oprice, " +  
				" en_0651b09_openpriceinfo_bbl.ftou_uprice, " +  
				" openpriceuserinfo.logincode As priceby, " +  
				" en_0651b10_resultinfo_bbl.ft_ascore, " +  
				" en_0651b10_resultinfo_bbl.ft_bscore, " +  
				" en_0651b91_statsinfo_bbl.stats_markid, " +  
				" en_0651b91_statsmarking.markname, " +  
				" en_0651b91_statsmarking.bgcolor + '_' + en_0651b91_statsmarking.fgcolor As statsmark_color, " +  
				" en_0651b91_statsmarking_kp.kp_markid, " +  
				" en_0651b91_statsmarking_kp.kp_markname, " +  
				" en_0651b91_statsmarking_kp.bgcolor + '_' + en_0651b91_statsmarking_kp.fgcolor As kp_color, " +  
				" en_0651b91_statsinfo_bbl.lbl_markid, " +  
				" en_0651b91_statsmarking_lbl.lbl_markname, " +  
				" en_0651b91_statsmarking_lbl.bgcolor + '_' + en_0651b91_statsmarking_lbl.fgcolor As lbl_color, " +  	  
				" en_0651b91_statsinfo_bbl.mtrule_markid, " +  
				" en_0651b91_statsmarking_mtrule.mtrule_markname, " +  
				" en_0651b91_statsmarking_mtrule.bgcolor + '_' + en_0651b91_statsmarking_mtrule.fgcolor As mtrule_color, " +  
				" en_0651b91_statsinfo_bbl.ntable_arankid, " +  
				" en_0651b91_statsinfo_bbl.ntable_brankid, " +  
				" en_0651b91_statsinfo_bbl.ntable_apoints, " +  
				" en_0651b91_statsinfo_bbl.ntable_bpoints, " +  
				" en_0651b91_statsinfo_bbl.ntable_arankid_ps, " +  
				" en_0651b91_statsinfo_bbl.ntable_brankid_ps, " +  
				" en_0651b91_statsinfo_bbl.ntable_aclass_ud, " +  
				" en_0651b91_statsinfo_bbl.ntable_bclass_ud, " +  
				" en_0651b91_statsinfo_bbl.ntable_arankid_dr, " +
				" en_0651b91_statsinfo_bbl.ntable_brankid_dr, " +
				" en_0651b91_statsinfo_bbl.ntable_apoints_dr, " +
				" en_0651b91_statsinfo_bbl.ntable_bpoints_dr, " +
				" en_0651b91_statsinfo_bbl.hdpwld_arankid, " +  
				" en_0651b91_statsinfo_bbl.hdpwld_brankid, " +  
				" en_0651b91_statsinfo_bbl.hdpwldha_arankid, " +  
				" en_0651b91_statsinfo_bbl.hdpwldha_brankid, " +  
				" en_0651b91_statsinfo_bbl.oufs_arankid, " +  
				" en_0651b91_statsinfo_bbl.oufs_brankid, " +  
				" en_0651b91_statsinfo_bbl.hdpwld_alast5, " +  
				" en_0651b91_statsinfo_bbl.hdpwld_blast5, " +  
				" en_0651b91_statsinfo_bbl.ou_alast5, " +  
				" en_0651b91_statsinfo_bbl.ou_blast5, " +  
				" en_0651b91_statsinfo_bbl.hdpwld_apattern, " +  
				" en_0651b91_statsinfo_bbl.hdpwld_bpattern, " +  
				" en_0651b91_statsinfo_bbl.hdpwldha_apattern, " +  
				" en_0651b91_statsinfo_bbl.hdpwldha_bpattern, " +  
				" en_0651b91_statsinfo_bbl.scorewldha_apattern, " +  
				" en_0651b91_statsinfo_bbl.scorewldha_bpattern, " +  
				" en_0651b91_statsinfo_bbl.s6_apattern, " +  
				" en_0651b91_statsinfo_bbl.s11_apattern, " +  
				" en_0651b08_scheduleinfo_bbl.scheduledate As matchorderdate " +  
			" From en_0651b08_scheduleinfo_bbl, en_0651b09_openpriceinfo_bbl, en_0651b10_resultinfo_bbl, " +  
				" en_0651b91_statsinfo_bbl, en_0651b01_leagueinfo_bbl, en_0651b06_seasoninfo_bbl, " +  
				" en_0651b03_teaminfo_bbl As hometeaminfo, en_0651b03_teaminfo_bbl As awayteaminfo, " +  
				" en_0651z00_eventinchargetype, " +  
				" en_0651b91_statsmarking, en_0651b91_statsmarking_kp, en_0651b91_statsmarking_lbl, " +  
				" en_0651b91_statsmarking_mtrule, en_0151a04_userinfo As scheduleuserinfo, " +   
				" en_0151a04_userinfo As openpriceuserinfo WITH (NOLOCK) " +  
			" Where en_0651b08_scheduleinfo_bbl.scheduleid = en_0651b09_openpriceinfo_bbl.scheduleid And " +  
				" en_0651b09_openpriceinfo_bbl.scheduleid = en_0651b10_resultinfo_bbl.scheduleid And " +  
				" en_0651b10_resultinfo_bbl.scheduleid = en_0651b91_statsinfo_bbl.scheduleid And " +  
				" en_0651b08_scheduleinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +  
				" en_0651b01_leagueinfo_bbl.leagueid = en_0651b06_seasoninfo_bbl.leagueid And " +  
				" en_0651b06_seasoninfo_bbl.seasonid = " + 
					" ( " +
						" Select Top 1 seasoninfo.seasonid " +  
						" From en_0651b06_seasoninfo_bbl As seasoninfo " +  
						" Where seasoninfo.leagueid = en_0651b08_scheduleinfo_bbl.leagueid And " +  
							" en_0651b08_scheduleinfo_bbl.scheduledate Between seasoninfo.startdate And " + 
							" seasoninfo.enddate " +  
						" Order By seasoninfo.enddate Desc " +
					" ) And " +  
				" en_0651b08_scheduleinfo_bbl.ateamid = hometeaminfo.teamid And " +  
				" en_0651b08_scheduleinfo_bbl.bteamid = awayteaminfo.teamid And " +  
				" en_0651b08_scheduleinfo_bbl.createdby = scheduleuserinfo.userid And " +  
				" IsNull (en_0651b08_scheduleinfo_bbl.inchargetypeid, 0) = en_0651z00_eventinchargetype.inchargetypeid And " +
				" en_0651b08_scheduleinfo_bbl.createdby = openpriceuserinfo.userid And " +  
				" en_0651b91_statsinfo_bbl.stats_markid = en_0651b91_statsmarking.markid And " +  
				" en_0651b91_statsinfo_bbl.kp_markid = en_0651b91_statsmarking_kp.kp_markid And " +  
				" en_0651b91_statsinfo_bbl.lbl_markid = en_0651b91_statsmarking_lbl.lbl_markid And " +  
				" en_0651b91_statsinfo_bbl.mtrule_markid = en_0651b91_statsmarking_mtrule.mtrule_markid " +
				getThreadCondition_ML (bThread) +
				getAdvanceFilter_BL () +
			oCondition + getMatchOrderBy ();

        return oSQL;
    }        
	
    private String getMatchListSQL_SB ()
    {
	    String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";
		String oScheduleDate = oFromDate;

	    String oScheduleIds = m_arrInfo_CF [f_ScheduleIds_CF];
        String oLeagueIds = m_arrInfo_MA [f_LeagueIds_MA];
        
		String oCondition = "";

		if (oLeagueIds.equals ("0") == false)
			oCondition += " And en_0651b08_scheduleinfo_bbl.leagueid In  (" + oLeagueIds + ") ";

		if (oScheduleIds.equals ("0") == false)
		{
			oCondition += " And en_0651b08_scheduleinfo_bbl.scheduleid In  (" + oScheduleIds + ") ";
			oFromDate = " DateAdd (Day, -15, '" + oFromDate + "') ";
			oToDate = " DateAdd (Day, 15, '" + oToDate + "') ";
		}
		else
		{
			oFromDate = "'" + oFromDate + "'";
			oToDate = "'" + oToDate + "'";
		}

		oCondition += " And en_0651c02_betinfo_bbl.settlementdate Between " + oFromDate + " And " + oToDate;

	    String oSQL = 
			" Select Distinct en_0651b08_scheduleinfo_bbl.scheduleid, " +
				" en_0651b08_scheduleinfo_bbl.ref_scheduleid, " +
				" Convert (nvarchar, en_0651b08_scheduleinfo_bbl.scheduledate, 103) As scheduledate, " +
				" Convert (nvarchar (5), en_0651b08_scheduleinfo_bbl.scheduledate, 108) As scheduletime, " +  
				" en_0651b06_seasoninfo_bbl.seasonid, " +
				" en_0651b06_seasoninfo_bbl.seasonname_" + getLanguage () + ", " +
				" en_0651b01_leagueinfo_bbl.leagueid, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + ", " +
				" 0 As ismainleague, " + 
				" en_0651b01_leagueinfo_bbl.iscup, " +
				" en_0651b01_leagueinfo_bbl.countryid, " +  
				" hometeaminfo.teamid As ateamid, " +
				" hometeaminfo.teamname_" + getLanguage () + " As ateamname_" + getLanguage () + ", " +
				" awayteaminfo.teamid As bteamid, " +
				" awayteaminfo.teamname_" + getLanguage () + " As bteamname_" + getLanguage () + ", " +
				" en_0651b08_scheduleinfo_bbl.islive, " +
				" en_0651b08_scheduleinfo_bbl.isneutral, " + 
				" en_0651z00_eventinchargetype.inchargetypeid, " +
				" en_0651z00_eventinchargetype.inchargecode, " +
				" scheduleuserinfo.logincode As scheduledby, " +
				" en_0651b09_openpriceinfo_bbl.ftah_ahandicap As ahandicap, " +
				" en_0651b09_openpriceinfo_bbl.ftah_bhandicap As bhandicap, " +
				" en_0651b09_openpriceinfo_bbl.ftah_aprice, " +
				" en_0651b09_openpriceinfo_bbl.ftah_bprice, " +
				" en_0651b09_openpriceinfo_bbl.ftou_handicap As ouhandicap, " +
				" en_0651b09_openpriceinfo_bbl.ftou_oprice, " +
				" en_0651b09_openpriceinfo_bbl.ftou_uprice, " +
				" openpriceuserinfo.logincode As priceby, " +
				" en_0651b10_resultinfo_bbl.ft_ascore, " +
				" en_0651b10_resultinfo_bbl.ft_bscore, " +
				" en_0651b91_statsinfo_bbl.stats_markid, " +
				" en_0651b91_statsmarking.markname, " +
				" en_0651b91_statsmarking.bgcolor + '_' + en_0651b91_statsmarking.fgcolor As statsmark_color, " +
				" en_0651b91_statsmarking_kp.kp_markid, " +
				" en_0651b91_statsmarking_kp.kp_markname, " +
				" en_0651b91_statsmarking_kp.bgcolor + '_' + en_0651b91_statsmarking_kp.fgcolor As kp_color, " +
				" en_0651b91_statsinfo_bbl.lbl_markid, " +
				" en_0651b91_statsmarking_lbl.lbl_markname, " +
				" en_0651b91_statsmarking_lbl.bgcolor + '_' + en_0651b91_statsmarking_lbl.fgcolor As lbl_color, " +
				" en_0651b91_statsinfo_bbl.mtrule_markid, " +
				" en_0651b91_statsmarking_mtrule.mtrule_markname, " +
				" en_0651b91_statsmarking_mtrule.bgcolor + '_' + en_0651b91_statsmarking_mtrule.fgcolor As mtrule_color, " +
				" en_0651b91_statsinfo_bbl.ntable_arankid, " +
				" en_0651b91_statsinfo_bbl.ntable_brankid, " +
				" en_0651b91_statsinfo_bbl.ntable_apoints, " +
				" en_0651b91_statsinfo_bbl.ntable_bpoints, " +
				" en_0651b91_statsinfo_bbl.ntable_arankid_ps, " +
				" en_0651b91_statsinfo_bbl.ntable_brankid_ps, " +
				" en_0651b91_statsinfo_bbl.ntable_aclass_ud, " +
				" en_0651b91_statsinfo_bbl.ntable_bclass_ud, " +
				" en_0651b91_statsinfo_bbl.ntable_arankid_dr, " +
				" en_0651b91_statsinfo_bbl.ntable_brankid_dr, " +
				" en_0651b91_statsinfo_bbl.ntable_apoints_dr, " +
				" en_0651b91_statsinfo_bbl.ntable_bpoints_dr, " +
				" en_0651b91_statsinfo_bbl.hdpwld_arankid, " +
				" en_0651b91_statsinfo_bbl.hdpwld_brankid, " +
				" en_0651b91_statsinfo_bbl.hdpwldha_arankid, " +
				" en_0651b91_statsinfo_bbl.hdpwldha_brankid, " +
				" en_0651b91_statsinfo_bbl.oufs_arankid, " +
				" en_0651b91_statsinfo_bbl.oufs_brankid, " +
				" en_0651b91_statsinfo_bbl.hdpwld_alast5, " +
				" en_0651b91_statsinfo_bbl.hdpwld_blast5, " +
				" en_0651b91_statsinfo_bbl.ou_alast5, " +
				" en_0651b91_statsinfo_bbl.ou_blast5, " +
				" en_0651b91_statsinfo_bbl.hdpwld_apattern, " +
				" en_0651b91_statsinfo_bbl.hdpwld_bpattern, " +
				" en_0651b91_statsinfo_bbl.hdpwldha_apattern, " +
				" en_0651b91_statsinfo_bbl.hdpwldha_bpattern, " +
				" en_0651b91_statsinfo_bbl.scorewldha_apattern, " +
				" en_0651b91_statsinfo_bbl.scorewldha_bpattern, " +
				" en_0651b91_statsinfo_bbl.s6_apattern, " +
				" en_0651b91_statsinfo_bbl.s11_apattern, " +
				" en_0651b08_scheduleinfo_bbl.scheduledate As matchorderdate " +
			" From en_0651c02_betinfo_bbl, en_0651b08_scheduleinfo_bbl, en_0651b09_openpriceinfo_bbl, " +
				" en_0651b10_resultinfo_bbl, en_0651b91_statsinfo_bbl, en_0651b01_leagueinfo_bbl, " +
				" en_0651b06_seasoninfo_bbl, en_0651z00_eventinchargetype, en_0651b03_teaminfo_bbl As hometeaminfo, " +
				" en_0651b03_teaminfo_bbl As awayteaminfo, en_0651b91_statsmarking, en_0651b91_statsmarking_kp, " +
				" en_0651b91_statsmarking_lbl, en_0651b91_statsmarking_mtrule, " +
				" en_0151a04_userinfo As scheduleuserinfo, en_0151a04_userinfo As openpriceuserinfo WITH (NOLOCK) " +
			" Where en_0651c02_betinfo_bbl.scheduleid = en_0651b08_scheduleinfo_bbl.scheduleid And " +
				" en_0651b08_scheduleinfo_bbl.scheduleid = en_0651b09_openpriceinfo_bbl.scheduleid And " +  
				" en_0651b09_openpriceinfo_bbl.scheduleid = en_0651b10_resultinfo_bbl.scheduleid And " +
				" en_0651b10_resultinfo_bbl.scheduleid = en_0651b91_statsinfo_bbl.scheduleid And " +
				" en_0651b08_scheduleinfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
				" en_0651b01_leagueinfo_bbl.leagueid = en_0651b06_seasoninfo_bbl.leagueid And " +
				" en_0651b06_seasoninfo_bbl.seasonid = ( " +
					" Select Top 1 seasoninfo.seasonid " +
					" From en_0651b06_seasoninfo_bbl As seasoninfo " +
					" Where seasoninfo.leagueid = en_0651b08_scheduleinfo_bbl.leagueid And " +
						" en_0651b08_scheduleinfo_bbl.scheduledate Between seasoninfo.startdate And seasoninfo.enddate " +
					" Order By seasoninfo.enddate Desc " +
				" ) And " +
				" en_0651b08_scheduleinfo_bbl.ateamid = hometeaminfo.teamid And " +
				" en_0651b08_scheduleinfo_bbl.bteamid = awayteaminfo.teamid And " +
				" en_0651b08_scheduleinfo_bbl.createdby = scheduleuserinfo.userid And " +
				" IsNull (en_0651b08_scheduleinfo_bbl.inchargetypeid, 0) = en_0651z00_eventinchargetype.inchargetypeid And " +
				" en_0651b08_scheduleinfo_bbl.createdby = openpriceuserinfo.userid And " +
				" en_0651b91_statsinfo_bbl.stats_markid = en_0651b91_statsmarking.markid And " + 
				" en_0651b91_statsinfo_bbl.kp_markid = en_0651b91_statsmarking_kp.kp_markid And " + 
				" en_0651b91_statsinfo_bbl.lbl_markid = en_0651b91_statsmarking_lbl.lbl_markid And " + 
				" en_0651b91_statsinfo_bbl.mtrule_markid = en_0651b91_statsmarking_mtrule.mtrule_markid And " +
				" en_0651b08_scheduleinfo_bbl.scheduledate > DateAdd (Month, -1, '" + oScheduleDate + "') And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 " +
				getAdvanceFilter_BL () +
				oCondition + getMatchOrderBy ();

        return oSQL;
    }
    
    private String getMatchOrderBy ()
	{	    
	    int nOrderById = convertToInt (m_arrInfo_MF [f_OrderById_MF]);

		String oOrderBy = " Order By matchorderdate, leaguename_" + getLanguage () + ", ahandicap, bhandicap ";
		if (nOrderById == 1)
		    oOrderBy = " Order By ahandicap, bhandicap, matchorderdate, leaguename_" + getLanguage ();
		    
		return oOrderBy;
	}
		
    private String getBetList (boolean bThread)
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
        
        try
		{
		    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);
		
		    String oSQL = "";
		          
			if (nReportTypeId == kRT_PendingBets)
				oSQL = getBetListSQL_PB (bThread);				
			else if (nReportTypeId == kRT_SettledBets)
                oSQL = getBetListSQL_SB ();

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_BetList)); 
				oBuffer.append (getStatusXML (T8_BetList, 1, "BetsByTeamBL:getBetList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T8_BetList, -1, "BetsByTeamBL:getBetList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T8_BetList, -1, "BetsByTeamBL:getBetList:" + oException.toString ()));
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
    
    private String getBetListSQL_PB (boolean bThread)
    {
		String oCondition = "";
		boolean bIsSpecialUser = m_arrInfo_CF [f_IsSpecialUser_CF].equals ("1");

		if (bIsSpecialUser)
			oCondition = getSpecialUserCondition_Group ();

		String oSQL = 
			" Select en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode, " + 
				" en_0651b04_smartgroupinfo.fgcolor, " +
				" en_0651b04_smartgroupinfo.bgcolor, " +
				" en_0151z00_currency.currencycode_" + getLanguage () + ", " +
				" en_0651c02_betstatus_bbl.pt_percent, " +
				" en_0151a04_userinfo.groupid, " +
				" en_0151a04_userinfo.logincode, " +
				" en_0651c02_betstatus_bbl.scheduleid As matchscheduleid, " +
				" en_0651c02_betstatus_bbl.accountid, " +
				" en_0651c02_betstatus_bbl.accountcode, " +
				" en_0651c02_betstatus_bbl.betrefid, " +
				" en_0651c02_betstatus_bbl.bettypeid, " +
				" en_0251z00_bettype_main.mainbettypeid, " +
				" en_0651c02_betstatus_bbl.bettype_short, " +
				" Case When en_0251z00_bettype_main.mainbettypeid = " + SportUtil.kBMBT_OverUnder + " And " +
					" en_0651c02_betstatus_bbl.selectionid = 2 Then 1 Else " +
					" en_0651c02_betstatus_bbl.selectionid End As sideid, " +
				" en_0651c02_betstatus_bbl.selectionid, " +
				" en_0651c02_betstatus_bbl.selectiontext, " +
				" en_0651c02_betstatus_bbl.islive, " +
				" -1 As alivescore, " +  
				" -1 As blivescore, " +
				" 0 As handicapid, " +
				" en_0651c02_betstatus_bbl.handicap, " +
				" en_0651c02_betstatus_bbl.oddstypeid, " +
				" en_0651c02_betstatus_bbl.price, " +
				" (en_0651c02_betstatus_bbl.stake * account_currencyrates.currencyrate) / group_currencyrates.currencyrate As stake, " +
				" en_0651c02_betstatus_bbl.winlosetype, " +
				" 0 As winlose, " +
				" 0 As issettled, " +
				" en_0651c02_betstatus_bbl.live_totalturnover, " +
				" en_0651c02_betstatus_bbl.live_totalwinlose, " +
				" en_0651c02_betstatus_bbl.live_winbetcount, " +
				" en_0651c02_betstatus_bbl.live_losebetcount, " +
				" 0 As live_drawbetcount, " +
				" en_0651c02_betstatus_bbl.nonl_totalwinlose, " +
				" en_0651c02_betstatus_bbl.nonl_totalturnover, " +
				" en_0651c02_betstatus_bbl.nonl_winbetcount, " +
				" en_0651c02_betstatus_bbl.nonl_losebetcount, " +
				" 0 As nonl_drawbetcount, " +
				" en_0651c02_betstatus_bbl.smartpercent_s8, " +
				" en_0651c02_betstatus_bbl.smartgrade_bgcolor, " +
				" en_0651b08_scheduleinfo_bbl.scheduledate As matchorderdate, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + ", " +
				" en_0651b09_openpriceinfo_bbl.ftah_ahandicap As ahandicap, " +
				" en_0651b09_openpriceinfo_bbl.ftah_bhandicap As bhandicap, " +
				" en_0651c02_betstatus_bbl.createddate As betorderdate " +
			" From en_0651c02_betstatus_bbl, en_0651b08_scheduleinfo_bbl, en_0651b04_smartgroupinfo, " +
				" en_0651b09_openpriceinfo_bbl, en_0651b01_leagueinfo_bbl, en_0251z00_bettype, " + 
				" en_0251z00_bettype_main, en_0151z00_currency, en_0151a04_userinfo, " +
				" en_0651z00_currencyrates As group_currencyrates, " +
			    " en_0651z00_currencyrates As account_currencyrates WITH (NOLOCK) " +
			" Where en_0651c02_betstatus_bbl.scheduleid = en_0651b08_scheduleinfo_bbl.scheduleid And " +
				" en_0651c02_betstatus_bbl.smartid = en_0651b04_smartgroupinfo.smartid And " +
				" en_0651b09_openpriceinfo_bbl.scheduleid = en_0651c02_betstatus_bbl.scheduleid And " +
				" en_0651b01_leagueinfo_bbl.leagueid = en_0651c02_betstatus_bbl.leagueid And " +
				" en_0251z00_bettype.bettypeid = en_0651c02_betstatus_bbl.bettypeid And " +
				" en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +				
				" en_0151z00_currency.currencyid = en_0651b04_smartgroupinfo.currencyid And " +
				" account_currencyrates.currencyid = en_0651c02_betstatus_bbl.currencyid And " +
				" group_currencyrates.currencyid = en_0151z00_currency.currencyid And " +
				" account_currencyrates.ratetypeid = " + ConstantsUtil.kRateT_GBP + " And " +
				" group_currencyrates.ratetypeid = " + ConstantsUtil.kRateT_GBP + " And " +
				" en_0151a04_userinfo.userid = en_0651c02_betstatus_bbl.createdby And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651b04_smartgroupinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				getDateCondition_PB ("en_0651b08_scheduleinfo_bbl") + 
				getGeneralCondition_BL ("en_0651c02_betstatus_bbl") +
				getThreadCondition_PB (bThread) + 
				getMoreFilter_F () + 
				getMoreFilter_SF () + 
				oCondition +
			getBetListOrderBy ();

        return oSQL;
    }
    	   
    private String getBetListSQL_SB ()
    {
        String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";        

	    String oScheduleIds = m_arrInfo_CF [f_ScheduleIds_CF];
		boolean bIsSpecialUser = m_arrInfo_CF [f_IsSpecialUser_CF].equals ("1");
		
		String oCondition = "";

		if (oScheduleIds.equals ("0") == false)
		{
			oCondition = " And en_0651c02_betinfo_bbl.scheduleid In (" + oScheduleIds + ") ";
			oFromDate = " DateAdd (Day, -15, '" + oFromDate + "') ";
			oToDate = " DateAdd (Day, 15, '" + oToDate + "') ";
		}
		else
		{
			oFromDate = "'" + oFromDate + "'";
			oToDate = "'" + oToDate + "'";
		}

		if (bIsSpecialUser)
			oCondition += getSpecialUserCondition_Group ();

		String oSQL = 
			" Select en_0651b04_smartgroupinfo.smartid, " +
				" en_0651b04_smartgroupinfo.groupcode, " + 
				" en_0651b04_smartgroupinfo.fgcolor, " +
				" en_0651b04_smartgroupinfo.bgcolor, " +
				" group_currencyinfo.currencycode_" + getLanguage () + ", " +
				" en_0651c02_betinfo_bbl.pt_percent, " +
				" en_0151a04_userinfo.groupid, " +
				" en_0151a04_userinfo.logincode, " +
				" en_0651c02_betinfo_bbl.scheduleid As matchscheduleid, " +
				" en_0251b02_accountinfo.accountid, " +
				" en_0251b02_accountinfo.accountcode, " +
				" en_0651c02_betinfo_bbl.betrefid, " +
				" en_0651c02_betinfo_bbl.bettypeid, " +
				" en_0251z00_bettype_main.mainbettypeid, " +
				" en_0251z00_bettype.shortname_" + getLanguage () + " As bettype_short, " +
				" Case When en_0251z00_bettype_main.mainbettypeid = " + SportUtil.kBMBT_OverUnder + " And " +
					" en_0651c02_betinfo_bbl.selectionid = 2 Then 1 Else " +
					" en_0651c02_betinfo_bbl.selectionid End As sideid, " +
				" en_0651c02_betinfo_bbl.selectionid, " +
				" en_0651c02_betinfo_bbl.selectiontext, " +
				" en_0651c02_betinfo_bbl.islive, " +
				" -1 As alivescore, " +  
				" -1 As blivescore, " +
				" 0 As handicapid, " + 
				" en_0651c02_betinfo_bbl.handicap, " +
				" en_0651c02_betinfo_bbl.oddstypeid, " +
				" en_0651c02_betinfo_bbl.price, " +
				" (en_0651c02_betinfo_bbl.stake * account_currencyrates.currencyrate) / group_currencyrates.currencyrate As stake, " +
				" en_0651c02_betinfo_bbl.winlosetype, " +
				" (en_0651c02_betinfo_bbl.winlose * account_currencyrates.currencyrate) / group_currencyrates.currencyrate As winlose, " +
				" 1 As issettled, " +
				" 0 As live_totalturnover, " +
				" 0 As live_totalwinlose, " +
				" 0 As live_winbetcount, " +
				" 0 As live_losebetcount, " +
				" 0 As live_drawbetcount, " +
				" 0 As nonl_totalwinlose, " +
				" 0 As nonl_totalturnover, " +
				" 0 As nonl_winbetcount, " +
				" 0 As nonl_losebetcount, " +
				" 0 As nonl_drawbetcount, " +
				" en_0651c02_betinfo_bbl.smartpercent_s8, " +
				" '#ffffff' As smartgrade_bgcolor, " +
				" en_0651c02_betinfo_bbl.scheduledate As matchorderdate, " +
				" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + ", " +
				" en_0651b09_openpriceinfo_bbl.ftah_ahandicap As ahandicap, " +
				" en_0651b09_openpriceinfo_bbl.ftah_bhandicap As bhandicap, " +
				" en_0651c02_betinfo_bbl.createddate As betorderdate " +
			" From en_0651c02_betinfo_bbl, en_0651b09_openpriceinfo_bbl, en_0251b02_accountinfo, " +
				" en_0651b04_smartgroupaccounts, en_0651b04_smartgroupinfo, en_0651b01_leagueinfo_bbl, " +
				" en_0151a04_userinfo, en_0251z00_bettype, en_0251z00_bettype_main, " +
				" en_0151z00_currency As group_currencyinfo, en_0151z00_currency As account_currencyinfo, " +
				" en_0651z00_currencyrates As group_currencyrates, en_0651z00_currencyrates As account_currencyrates WITH (NOLOCK) " +				
			" Where en_0651c02_betinfo_bbl.scheduleid = en_0651b09_openpriceinfo_bbl.scheduleid And " +
				" en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
				" en_0251b02_accountinfo.accountid = en_0651b04_smartgroupaccounts.accountid And " +
				" en_0651b04_smartgroupaccounts.smartid = en_0651b04_smartgroupinfo.smartid And " +
				" en_0151a04_userinfo.userid = en_0651c02_betinfo_bbl.createdby And " +
				" en_0251z00_bettype.bettypeid = en_0651c02_betinfo_bbl.bettypeid And " +
				" en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
				" en_0651b01_leagueinfo_bbl.leagueid = en_0651c02_betinfo_bbl.leagueid And " +
			    " group_currencyinfo.currencyid = en_0651b04_smartgroupinfo.currencyid And " +
				" account_currencyinfo.currencyid = en_0251b02_accountinfo.currencyid And " +
				" group_currencyrates.currencyid = group_currencyinfo.currencyid And " +
				" account_currencyrates.currencyid = account_currencyinfo.currencyid And " +
	            " account_currencyrates.ratetypeid = " + ConstantsUtil.kRateT_GBP + " And " +
	            " group_currencyrates.ratetypeid = " + ConstantsUtil.kRateT_GBP + " And " +
				" en_0651c02_betinfo_bbl.settlementby > 0 And " +
				" en_0651b04_smartgroupaccounts.isreplicated = 0 And " +
				" en_0651b04_smartgroupinfo.sportid = " + SportUtil.kS_BasketBall + " And " +
				" en_0651c02_betinfo_bbl.settlementdate Between " + oFromDate + " And " + oToDate + " And " +
				" en_0251b02_accountinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
				getGeneralCondition_BL ("en_0251b02_accountinfo") + oCondition + 
				getMoreFilter_F () + 
				getMoreFilter_PF () + 
				getMoreFilter_SF () + 
                TradingUtil.getBetListCondition_DR (this, getUserId (), m_arrInfo_CF [f_CompanyUnitId_CF], "en_0651c02_betinfo_bbl") +				
			getBetListOrderBy ();

        return oSQL;
    }
    
    private String getBetListOrderBy ()
	{
		String oOrderBy = " Order By matchorderdate, leaguename_" + getLanguage () + ", ahandicap, bhandicap, " +
			" matchscheduleid, betorderdate ";
		
		return oOrderBy;
	}

 	private String getDateCondition_PB (String oTableName)
	{
	    String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
        String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";

		String oCondition = " And " + oTableName + ".scheduledate > DateAdd (Minute, -" + ConstantsUtil.kTimeCutOff_Bbl + ", GetDate ()) " +
			" And DateAdd (Minute, " + ConstantsUtil.kOffsetMinutes + ", " + oTableName + ".scheduledate) " +
			" Between '" + oFromDate + "' And DateAdd (Minute, " + ConstantsUtil.kExtraMinutes + ", '" + oToDate + "') ";

		return oCondition;
	}

    private String getGeneralCondition_BL (String oTableName)
    {
        String oCondition = getIgnoreClientCondition (oTableName);

        String oBetTypeIds = SportUtil.kBMBT_Handicap + ", " + SportUtil.kBMBT_OverUnder;
		oCondition += " And en_0251z00_bettype.mainbettypeid In (" + oBetTypeIds + ") ";

        UserSmartMasterPM_T1 objSmartMasterPM = new UserSmartMasterPM_T1 (getDocument ());
        String oSmartMasterIds_PM = objSmartMasterPM.getUserSmartMasterIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (m_arrInfo_CF [f_CompanyUnitId_CF]));

        if (oSmartMasterIds_PM.equals ("0") == false)
        {
			oSmartMasterIds_PM = oSmartMasterIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.mastergroupid) + '_', Convert (nvarchar (max), '_" + oSmartMasterIds_PM + "_')) > 0 ";
        }

		UserSmartGroupPM_T1 objSmartGroupPM = new UserSmartGroupPM_T1 (getDocument ());
		String oSmartGroupIds_PM = objSmartGroupPM.getUserSmartGroupIds_PM (getUserId (), kModuleId, SportUtil.kS_BasketBall, convertToInt (m_arrInfo_CF [f_CompanyUnitId_CF]));			

		if (oSmartGroupIds_PM.equals ("0") == false)
		{
			oSmartGroupIds_PM = oSmartGroupIds_PM.replaceAll(", ", "_");
			oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0651b04_smartgroupinfo.smartid) + '_', Convert (nvarchar (max), '_" + oSmartGroupIds_PM + "_')) > 0 ";
		}

		String oMasterIds = m_arrInfo_MA [f_SmartMasterIds_MA];
		if (oMasterIds.equals ("0") == false)
			oCondition += " And en_0651b04_smartgroupinfo.mastergroupid In (" + oMasterIds + ") ";
		
		String oGroupIds = m_arrInfo_MA [f_SmartGroupIds_MA];
		if (oGroupIds.equals ("0") == false)
			oCondition += " And en_0651b04_smartgroupinfo.smartid In (" + oGroupIds + ") ";

		String oLeagueIds = m_arrInfo_MA [f_LeagueIds_MA];
		if (oLeagueIds.equals ("0") == false)
			oCondition += " And en_0651b01_leagueinfo_bbl.leagueid In (" + oLeagueIds + ") ";

		String oMainBetTypeIds = m_arrInfo_MA [f_MainBetTypeIds_MA];
		if (oMainBetTypeIds.equals ("0") == false)
			oCondition += " And en_0251z00_bettype.mainbettypeid In (" + oMainBetTypeIds + ") ";

		return oCondition;
    }

	private String getMoreFilter_F ()
	{
	    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);

	    int nWinLoseTypeId_MF = convertToInt (m_arrInfo_MF [f_WinLoseTypeId_MF]);
	    int nLiveStatusId_MF = convertToInt (m_arrInfo_MF [f_LiveStatusId_MF]);
	    int nVIPTypeId_MF = convertToInt (m_arrInfo_MF [f_VIPTypeId_MF]);
	    
		String oCondition = "";
		if (nVIPTypeId_MF > -1)
            oCondition = " And en_0651b04_smartgroupinfo.isvip = " + nVIPTypeId_MF;
		    
        if (nReportTypeId == kRT_PendingBets)
        {
            if (nLiveStatusId_MF > -1)
                oCondition += " And en_0651c02_betstatus_bbl.islive = " + nLiveStatusId_MF;
        }
        else if (nReportTypeId == kRT_SettledBets)
        {
            if (nLiveStatusId_MF > -1)
                oCondition += " And en_0651c02_betinfo_bbl.islive = " + nLiveStatusId_MF;

            if (nWinLoseTypeId_MF > 0)
            {
                if (nWinLoseTypeId_MF == 1) // Win Bets
                    oCondition += " And en_0651c02_betinfo_bbl.winlosetype > 1 ";
                else if (nWinLoseTypeId_MF == 2) // Lose Bets
                    oCondition += " And en_0651c02_betinfo_bbl.winlosetype < 1 ";
                else if (nWinLoseTypeId_MF == 3) // Draw Bets
                    oCondition += " And en_0651c02_betinfo_bbl.winlosetype = 1 ";
            }
        }
                
        return oCondition;
	}		

	private String getMoreFilter_PF ()
	{
		int nLastMonthId_MF = convertToInt (m_arrInfo_MF [f_LastMonthId_MF]);
		String oTopPerformanceId_MF = m_arrInfo_MF [f_TopPerformanceId_MF];
		String oMinimumBetsId_MF = m_arrInfo_MF [f_MinimumBetsId_MF];
	    
		String oCondition = "";
        if (nLastMonthId_MF > 0)
        {
			String [] arrSmartFltrs = new String [] 
			{
				"0",	// MainBetTypeId
				"0",	// LeagueId
				"" + nLastMonthId_MF, 
				oTopPerformanceId_MF, 
				oMinimumBetsId_MF
			};

			SmartPerfFltrsObj oSmartPerfFltrsObj = new SmartPerfFltrsObj (getDocument ());
			String oSmartIds = oSmartPerfFltrsObj.getSmartIds (SportUtil.kS_BasketBall, arrSmartFltrs);
            oCondition += " And en_0651b04_smartgroupinfo.smartid In (" + oSmartIds + ")";

			oSmartPerfFltrsObj = null;
        }
                        
        return oCondition;
	}		
    
	private String getMoreFilter_SF ()
	{
	    int nReportTypeId = convertToInt (m_arrInfo_CF [f_ReportTypeId_CF]);

	    int nSelectionBTId_MF = convertToInt (m_arrInfo_MF [f_SelectionBTId_MF]);
	    int nSelectionId_MF = convertToInt (m_arrInfo_MF [f_SelectionId_MF]);
	    
		String oCondition = "";
        if (nSelectionBTId_MF > 0)
        {
            oCondition += " And en_0251z00_bettype_main.mainbettypeid = " + nSelectionBTId_MF;

            if (nReportTypeId == kRT_PendingBets && nSelectionId_MF > 0)
            {
                oCondition += " And en_0651c02_betstatus_bbl.selectionid = " + nSelectionId_MF;
            }
            else if (nReportTypeId == kRT_SettledBets && nSelectionId_MF > 0)
            {
                oCondition += " And en_0651c02_betinfo_bbl.selectionid = " + nSelectionId_MF;
            }
        }
                        
        return oCondition;
	}		
        
    private String getIgnoreClientCondition (String oTableName)
    {
        String oCondition = " And " + oTableName + ".clientid Not In ( " +
            " Select en_0651c91_ignoreclients_bbt.clientid From en_0651c91_ignoreclients_bbt) ";        
            
        return oCondition;
    }            
    
	private String getThreadCondition_PB (boolean bThread)
	{
		String oCondition = "";
		
		if (bThread)
        {
		    String oCurrentTimeStamp = m_arrInfo_MA [f_CurrentTimeStamp_MA];
		    oCurrentTimeStamp = oCurrentTimeStamp.equals ("notset") ? " GetDate () " : " '" + oCurrentTimeStamp + "' ";

            String oMatchIds = getRefreshMatchIds (oCurrentTimeStamp);
		    oCondition = " And en_0651c02_betstatus_bbl.scheduleid In (" + oMatchIds + ") ";
        } 
        
        return oCondition;
	}		

	private String getThreadCondition_ML (boolean bThread)
	{
		String oCondition = "";
		
		if (bThread)
        {
		    String oCurrentTimeStamp = m_arrInfo_MA [f_CurrentTimeStamp_MA];
		    oCurrentTimeStamp = oCurrentTimeStamp.equals ("notset") ? " GetDate () " : " '" + oCurrentTimeStamp + "' ";
		    oCondition = " And en_0651b91_statsinfo_bbl.modifieddate >= " + oCurrentTimeStamp;
        } 
        
        return oCondition;
	}		

	private String getAdvanceFilter_BL ()
	{
		int nAHWeightageId_BLF_AF = convertToInt (m_arrInfo_AF [f_AHWeightageId_BLF_AF]);
	    int nAHPricingId_BLF_AF = convertToInt (m_arrInfo_AF [f_AHPricingId_BLF_AF]);    
	    int nTWeightageId_BLF_AF = convertToInt (m_arrInfo_AF [f_TWeightageId_BLF_AF]);
	    int nMTRuleId_BLF_AF   = convertToInt (m_arrInfo_AF [f_MTRuleId_BLF_AF]);

		String oCondition = "";
		if (nAHWeightageId_BLF_AF > 0)
	        oCondition = " And en_0651b91_statsinfo_bbl.stats_markid = " + nAHWeightageId_BLF_AF;
        
		if (nAHPricingId_BLF_AF > 0)
	        oCondition += " And en_0651b91_statsinfo_bbl.lbl_markid = " + nAHPricingId_BLF_AF;

		if (nTWeightageId_BLF_AF > 0)
	        oCondition += " And en_0651b91_statsinfo_bbl.kp_markid = " + nTWeightageId_BLF_AF;		

		if (nMTRuleId_BLF_AF > 0)
	        oCondition += " And en_0651b91_statsinfo_bbl.mtrule_markid = " + nMTRuleId_BLF_AF;

        return oCondition;
	}		

	private String getRefreshMatchIds (String oCurrentTimeStamp)
    {
		DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;
    
        StringBuffer oMatchIdsBuffer = new StringBuffer ("-1");
        
        try
        {
            String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
		    String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";
            
            String oSQL =             
                " Select Distinct en_0651c02_betstatus_bbl.scheduleid " +
                " From en_0651c02_betstatus_bbl, en_0651b04_smartgroupinfo, " +
					" en_0651b01_leagueinfo_bbl, en_0251z00_bettype  WITH (NOLOCK) " +
                " Where en_0651c02_betstatus_bbl.smartid = en_0651b04_smartgroupinfo.smartid And " +
					" en_0651c02_betstatus_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +
					" en_0651c02_betstatus_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
					" en_0651b04_smartgroupinfo.unitid = " + m_arrInfo_CF [f_CompanyUnitId_CF] +
					getDateCondition_PB ("en_0651c02_betstatus_bbl") + " And " +
	                " en_0651c02_betstatus_bbl.createddate >= " + oCurrentTimeStamp +
				getGeneralCondition_BL ("en_0651c02_betstatus_bbl");

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)
			{
				while (oResultSet.next ())   
					oMatchIdsBuffer.append ("," + oResultSet.getString ("scheduleid"));
			}
        }
        catch (Exception oException)
        {
            log ("BetsByTeamBL:getRefreshMatchIds:" + oException.toString ());
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

        return oMatchIdsBuffer.toString ();
    }

	private String getFinishedMatchIds ()
    {
		DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;
    
        StringBuffer oBuffer = new StringBuffer ("");
        
        try
        {
            String oFromDate = m_arrInfo_CF [f_FromDate_CF] + " 00:00:00";
		    String oToDate = m_arrInfo_CF [f_ToDate_CF] + " 23:59:59";
		    
            int nTimeCutOff = ConstantsUtil.kTimeCutOff_Bbl;
            nTimeCutOff = (nTimeCutOff > 0) ? nTimeCutOff * -1 : nTimeCutOff;
            
            String oSQL =             
				" Select en_0651b08_scheduleinfo_bbl.scheduleid " +
				" From en_0651b08_scheduleinfo_bbl " +
				" Where DateAdd (Minute, " + ConstantsUtil.kOffsetMinutes + ", en_0651b08_scheduleinfo_bbl.scheduledate) " +
					" Between '" + oFromDate + "' And DateAdd (Minute, " + ConstantsUtil.kExtraMinutes + ", '" + oToDate + "') And " +
					" en_0651b08_scheduleinfo_bbl.scheduledate < DateAdd (Minute, " + nTimeCutOff + ", GetDate ()) And " +
					" en_0651b08_scheduleinfo_bbl.scheduledate > DateAdd (Minute, -200, GetDate ()) " +
				" Order By scheduledate Desc ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_FinishedMatchIds)); 
				oBuffer.append (getStatusXML (T8_FinishedMatchIds, 1, "BetsByTeamBL:getFinishedMatchIds:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T8_FinishedMatchIds, -1, "BetsByTeamBL:getFinishedMatchIds:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T8_FinishedMatchIds, -1, "BetsByTeamBL:getFinishedMatchIds:" + oException.toString ()));
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

    private String getCurrentTimeStamp ()
    {
        DBConnector10651 oConnector = new DBConnector10651 ();
		Statement oStatement = null;
		ResultSet oResultSet = null;		

        StringBuffer oBuffer = new StringBuffer ();
        try
        {
            String oSQL = 
                " Select Replace (Convert (nvarchar, GetDate (), 111), '/', '-') + ' ' + " + 
			        " Convert (nvarchar, GetDate (), 114) As timestampvalue ";
			        
			oStatement = oConnector.getStatement ();			
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
            if (oResultSet != null)
            {
                oBuffer.append (toXMLString (oResultSet, kSR_TimeStamp));
                oBuffer.append (getStatusXML (T8_TimeStamp, 1, "BetsByTeamBL:getCurrentTimeStamp:Successful"));
            }
            else
                oBuffer.append (getStatusXML (T8_TimeStamp, -1, "BetsByTeamBL:getCurrentTimeStamp:Unsuccessful"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T8_TimeStamp, -1, "BetsByTeamBL:getCurrentTimeStamp" + oException.toString ()));
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
  
   /* Common Data ===================================================================== */
    private String getInitData_Z ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        String [] arrInfo = getParams (StatsConstants.kCR_Z_InitData);
		String oLeagueId = arrInfo [StatsConstants.f_LeagueId_SD];
		String oSeasonId = arrInfo [StatsConstants.f_SeasonId_SD];
		String oCountryId = arrInfo [StatsConstants.f_CountryId_SD];
        
        oBuffer.append (getLeaguesByCountry_Z (oCountryId));
        oBuffer.append (getSeasons_Z (oLeagueId));
        oBuffer.append (getTeams_Z (oLeagueId, oSeasonId));
        
        oBuffer.append (getTeamStatsHome_A ());
        oBuffer.append (getTeamStatsAway_A ());
        
        return oBuffer.toString ();
    }

    private String getLeaguesByCountry_Z (String oCountryId)
    {
        DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
		
        try
        {			
			String oSQL =
				" Select en_0651b01_leagueinfo_bbl.leagueid, " + 
					" en_0651b01_leagueinfo_bbl.leaguename_" + getLanguage () + " As leaguename, " +  					
					" 1 As orderid " +  					
				" From en_0651b01_leagueinfo_bbl " +  
				" Where en_0651b01_leagueinfo_bbl.leagueid > 0 And " +   
				    " en_0651b01_leagueinfo_bbl.isprocessed > 0 And " +
				    "en_0651b01_leagueinfo_bbl.countryid = " + oCountryId +
				" Union All " +  
				" Select -1 As leagueid, " +  
					" en_0651z00_firstelement.name_" + getLanguage () + " As leaguename, " +  
					" 0 As orderid " +  
				" From en_0651z00_firstelement " +  
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_ChooseOne +
				" Order By orderid, leaguename ";				                			

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, StatsConstants.kSR_Z_LeagueList)); 
				oBuffer.append (getStatusXML (StatsConstants.T101_LeagueList, 1, "BetsByTeamBL:getLeaguesByCountry_Z:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (StatsConstants.T101_LeagueList, -1, "BetsByTeamBL:getLeaguesByCountry_Z:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (StatsConstants.T101_LeagueList, -1, "BetsByTeamBL:getLeaguesByCountry_Z:" + oException.toString ()));
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
       
    private String getSeasonData_Z ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        
        String [] arrInfo = getParams (StatsConstants.kCR_Z_LeagueId);
		String oLeagueId = arrInfo [StatsConstants.f_LeagueId_SE];
        
        oBuffer.append (getSeasons_Z (oLeagueId));
        
        return oBuffer.toString ();
    }
    
    private String getTeamData_Z ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        
        String [] arrInfo = getParams (StatsConstants.kCR_Z_Teams);
		String oLeagueId = arrInfo [StatsConstants.f_LeagueId_TE];
		String oSeasonId = arrInfo [StatsConstants.f_SeasonId_TE];
        
        oBuffer.append (getTeams_Z (oLeagueId, oSeasonId));
        
        return oBuffer.toString ();
    }
    
    private String getSeasons_Z (String oLeagueId)
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
		
        try
        {			
			String oSQL =
				" Select en_0651b06_seasoninfo_bbl.seasonid As id, " +
				   " en_0651b06_seasoninfo_bbl.seasonname_" + getLanguage () + " As name, " +
				   " Convert (nvarchar, en_0651b06_seasoninfo_bbl.startdate, 103) As startdate, " +
				   " Convert (nvarchar, en_0651b06_seasoninfo_bbl.enddate, 103) As enddate, " +
				   " en_0651b06_seasoninfo_bbl.startdate As orderdate, " +
				   " 1 As orderid " +
				" From en_0651b06_seasoninfo_bbl, en_0651b01_leagueinfo_bbl " +
				" Where en_0651b06_seasoninfo_bbl.leagueid = en_0651b01_leagueinfo_bbl.leagueid And " +				   
				   " en_0651b01_leagueinfo_bbl.leagueid = " + oLeagueId +
				" Union All " +
				" Select 0 As id, " +
				   " en_0651z00_firstelement.name_" + getLanguage () + " As name, " +
				   " Convert (nvarchar, GetDate (), 103) As startdate, " +
				   " Convert (nvarchar, GetDate (), 103) As enddate, " +
				   " GetDate () As orderdate, " +
				   " 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_ChooseOne +
				" Order By orderid, orderdate Desc, name ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, StatsConstants.kSR_Z_SeasonList)); 
				oBuffer.append (getStatusXML (StatsConstants.T102_SeasonList, 1, "BetsByTeamBL:getSeasons_Z:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (StatsConstants.T102_SeasonList, -1, "BetsByTeamBL:getSeasons_Z:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (StatsConstants.T102_SeasonList, -1, "BetsByTeamBL:getSeasons_Z:" + oException.toString ()));
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

	private String getTeams_Z (String oLeagueId, String oSeasonId)
    {
		DBConnector10651 oConnector = new DBConnector10651 ();        
		Statement oStatement = null;
		ResultSet oResultSet = null;
		
		StringBuffer oBuffer = new StringBuffer ();
		
        try
        {			
			String oSQL =
				" Select Distinct en_0651b03_teaminfo_bbl.teamid As id, " +
					" en_0651b03_teaminfo_bbl.teamname_" + getLanguage () + " As name, " +
					" 1 As orderid " +
				" From en_0651b03_teaminfo_bbl, en_0651b08_scheduleinfo_bbl, en_0651b06_seasoninfo_bbl " +
				" Where en_0651b08_scheduleinfo_bbl.leagueid = en_0651b06_seasoninfo_bbl.leagueid And " +
					" en_0651b08_scheduleinfo_bbl.leagueid = " + oLeagueId + " And " +
					" (en_0651b08_scheduleinfo_bbl.ateamid = en_0651b03_teaminfo_bbl.teamid OR " +
						" en_0651b08_scheduleinfo_bbl.bteamid = en_0651b03_teaminfo_bbl.teamid) And " +
					" en_0651b06_seasoninfo_bbl.seasonid = " + oSeasonId + " And " +
					" en_0651b08_scheduleinfo_bbl.scheduledate Between " +					
						" en_0651b06_seasoninfo_bbl.startdate And en_0651b06_seasoninfo_bbl.enddate " +
				" Union All " +
				" Select 0 As id, " +
				   " en_0651z00_firstelement.name_" + getLanguage () + " As name, " +
				   " 0 As orderid " +
				" From en_0651z00_firstelement " +
				" Where en_0651z00_firstelement.id = " + ConstantsUtil.kFE_ChooseOne +
				" Order By orderid, name ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, StatsConstants.kSR_Z_TeamList)); 
				oBuffer.append (getStatusXML (StatsConstants.T103_TeamList, 1, "BetsByTeamBL:getTeams_Z:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (StatsConstants.T103_TeamList, -1, "BetsByTeamBL:getTeams_Z:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (StatsConstants.T103_TeamList, -1, "BetsByTeamBL:getTeams_Z:" + oException.toString ()));
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

    private String getTeamStatsHome_A ()
    {
		String [] arrInfo = getParams (kCR_A_TeamStats_Home);				
		String oXMLString = "";
		
		if (arrInfo != null)
		{
		    TeamStatsObj oLogic = new TeamStatsObj (getDocument ());
		    oXMLString = oLogic.getTeamStats (SportUtil.kS_BasketBall, arrInfo);		
		}
		
		return oXMLString;
    }

    private String getTeamStatsAway_A ()
    {
		String [] arrInfo = getParams (kCR_A_TeamStats_Away);
		String oXMLString = "";
		
		if (arrInfo != null)
		{
		    TeamStatsObj oLogic = new TeamStatsObj (getDocument ());
		    oXMLString = oLogic.getTeamStats (SportUtil.kS_BasketBall, arrInfo);
		}
		
		return oXMLString;
    }
    
    /* ===================================================================== Common Data */
    
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