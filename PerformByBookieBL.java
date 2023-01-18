package enlj.p106trading.mssqlv51.p10651basketball.pbperformbybookie.logics;

import java.sql.*;
import org.w3c.dom.*;

import enlj.projenv.logics.*;
import enlj.webenv.logics.*;
import enlj.projenv.mssql.*;
import enlj.webenv.utils.*;
import enlj.p101admin.commonsv11.resource.logics.userpm.*;
import enlj.p106trading.mssqlv51.resource.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.cbookieperformance.logics.*;
import enlj.p106trading.mssqlv51.resource.module.m10651.aperformance.fleagueperformance.logics.*;

public class PerformByBookieBL extends MSELogic
{
    /* Module Id */
    private final String kModuleId = "106510409";    	

	/* Task Ids */
	private final String kInitData				= "10651040901";	
	private final String kMainBetTypes			= "10651040904";
	private final String kLeagues				= "10651040905";
	private final String kBookies				= "10651040906";
	private final String kBookiePerformance		= "10651040907";
	private final String kAccountPerformance	= "10651040908";
	private final String kLeaguePerformance		= "10651040909";

	private final String kA_LeaguePerfByBookie	= "10651040911";
	private final String kB_BookiePerfByLeague	= "10651040921";

	/* Server Row Names */
	private final String kSR_Currencies		= "sr1";
	private final String kSR_MainBetTypes	= "sr2";	
    private final String kSR_Leagues		= "sr3";
	private final String kSR_Bookies		= "sr4";

	private final String kSR_CompanyUnits	= "sr8";

	/* Client Row Names */	
    private final String kCR_CommonList		= "cr1";
	private final String kCR_Performance	= "cr2";

	private final String kCR_A_League	= "cr11";
	private final String kCR_B_Bookie	= "cr21";

	/* Status Ids */
	private final String T1_FunctionPM  = "101";
	private final String T1_Currencies  = "102";
	private final String T1_CompanyUnits= "103";

	private final String T4_MainBetTypes= "401";
	private final String T5_Leagues	    = "501";
	private final String T6_Bookies		= "601";
	
	/* Common List Fields */
	private final int f_FromDate_CL		= 0;
	private final int f_ToDate_CL		= 1;
    private final int f_CompanyUnitId_CL= 2;

   	public PerformByBookieBL ()
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
		else if (oTaskId.equals (kBookies))
        {
            oXMLString = getBookieList ();
        }
		else if (oTaskId.equals (kBookiePerformance))
        {
            oXMLString = getBookiePerformance ();
        }
		else if (oTaskId.equals (kAccountPerformance))
        {
            oXMLString = getAccountPerformance ();
        }
		else if (oTaskId.equals (kLeaguePerformance))
        {
            oXMLString = getLeaguePerformance ();
        }
		else if (oTaskId.equals (kA_LeaguePerfByBookie))
        {
            oXMLString = getLeaguePerform_A ();
        }
		else if (oTaskId.equals (kB_BookiePerfByLeague))
        {
            oXMLString = getBookiePerformance_B ();
        }        
       
		return oXMLString;
	}
	
	private String getInitData ()
    {
        StringBuffer oBuffer = new StringBuffer ();
        oBuffer.append (getFunctionPM (getUserId (), kModuleId, T1_FunctionPM));        
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
					" en_0251z00_bettype_main.mainbettype_en As mainbettype " +
				" From en_0651c02_betinfo_bbl, en_0251z00_bettype_main, en_0251b02_accountinfo, en_0251z00_bettype " +
				" Where en_0651c02_betinfo_bbl.bettypeid = en_0251z00_bettype.bettypeid And " +
					" en_0651c02_betinfo_bbl.accountid = en_0251b02_accountinfo.accountid And " +
	                " en_0251b02_accountinfo.unitid = " + arrInfo [f_CompanyUnitId_CL] + " And " +
					" en_0251z00_bettype_main.mainbettypeid = en_0251z00_bettype.mainbettypeid And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0251z00_bettype.sportid = " + SportUtil.kS_BasketBall +
				" Order By mainbettypeid ";

			oStatement = oConnector.getStatement ();

			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_MainBetTypes)); 
				oBuffer.append (getStatusXML (T4_MainBetTypes, 1, "PerformByBookieBL:getMainBetTypes:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "PerformByBookieBL:getMainBetTypes:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T4_MainBetTypes, -1, "PerformByBookieBL:getMainBetTypes:" + oException.toString ()));
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
				oBuffer.append (getStatusXML (T5_Leagues, 1, "PerformByBookieBL:getLeagueList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T5_Leagues, -1, "PerformByBookieBL:getLeagueList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T5_Leagues, -1, "PerformByBookieBL:getLeagueList:" + oException.toString ()));
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

	private String getBookieList ()
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
			int nUnitId = convertToInt (arrInfo [f_CompanyUnitId_CL]);
			
			String oCondition = "";
			
			UserBookiePM_T1 objBookiePM = new UserBookiePM_T1 (getDocument ());
			String oBookieIds_PM = objBookiePM.getUserBookieIds_PM (getUserId (), kModuleId, nUnitId);

			if (oBookieIds_PM.equals ("0") == false)
			{
				oBookieIds_PM = oBookieIds_PM.replaceAll(", ", "_");
				oCondition += " And CHARINDEX ('_' + Convert (nvarchar, en_0251b01_bookieinfo.bookieid) + '_', Convert (nvarchar (max), '_" + oBookieIds_PM + "_')) > 0 ";
			}

			String oSQL =
				" Select Distinct en_0251b01_bookieinfo.bookieid, " +     
					" en_0251b01_bookieinfo.bookiename " +
				" From en_0251b02_accountinfo, en_0651c02_betinfo_bbl, en_0251b01_bookieinfo " +
				" Where en_0251b02_accountinfo.accountid = en_0651c02_betinfo_bbl.accountid And " + 
	                " en_0251b02_accountinfo.unitid = " + nUnitId + " And " +			
					" en_0251b02_accountinfo.bookieid = en_0251b01_bookieinfo.bookieid And " +
					" en_0651c02_betinfo_bbl.settlementdate Between '" + oFromDate + "' And '" + oToDate + "' And " +
					" en_0251b01_bookieinfo.isactive = 1 " + oCondition + 
				" Order By bookiename ";

			oStatement = oConnector.getStatement ();
			oResultSet = oConnector.executeQuery (oSQL, oStatement);
			
			if (oResultSet != null)   
			{
			    oBuffer.append (toXMLString (oResultSet, kSR_Bookies)); 
				oBuffer.append (getStatusXML (T6_Bookies, 1, "PerformByBookieBL:getBookieList:Successfull"));
			}
			else
				oBuffer.append (getStatusXML (T6_Bookies, -1, "PerformByBookieBL:getBookieList:UnSuccessfull"));
        }
        catch (Exception oException)
        {
            oBuffer.append (getStatusXML (T6_Bookies, -1, "PerformByBookieBL:getBookieList:" + oException.toString ()));
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
	
	private String getBookiePerformance ()
    {

		String [] arrInfo = getParams (kCR_Performance);
        BookiePerformanceObj oLogic = new BookiePerformanceObj (getDocument ());

        String oXMLString = oLogic.getBookiePerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getAccountPerformance ()
    {

		String [] arrInfo = getParams (kCR_Performance);
        BookiePerformanceObj oLogic = new BookiePerformanceObj (getDocument ());

        String oXMLString = oLogic.getAccountPerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getLeaguePerformance ()
    {

		String [] arrInfo = getParams (kCR_Performance);
        BookiePerformanceObj oLogic = new BookiePerformanceObj (getDocument ());

        String oXMLString = oLogic.getLeaguePerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	private String getLeaguePerform_A ()
    {
		StringBuffer oBuffer = new StringBuffer ();

        LeaguePerformanceObj oLogic = new LeaguePerformanceObj (getDocument ());
		NodeList arrNodes = getChildNodes (kCR_A_League);

		for (int nIndex = 0; nIndex < arrNodes.getLength (); nIndex++)
		{
			Node oNode = arrNodes.item (nIndex);
			String [] arrInfo = getParams (oNode);

	        oBuffer.append (oLogic.getLeaguePerfBookie (SportUtil.kS_BasketBall, arrInfo));
		}

        return oBuffer.toString ();
	}	

	private String getBookiePerformance_B ()
    {

		String [] arrInfo = getParams (kCR_B_Bookie);
        BookiePerformanceObj oLogic = new BookiePerformanceObj (getDocument ());

        String oXMLString = oLogic.getBookiePerformance (SportUtil.kS_BasketBall, arrInfo);
        return oXMLString;
	}

	public void log (String oMessage)
	{
//		logMessage (oMessage);
	}
}